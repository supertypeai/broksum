from datetime import date, datetime
from zoneinfo import ZoneInfo

from scrape import (
    DATA_DIR,
    cmd_scrape,
    cmd_upload,
    get_supabase,
    latest_trading_day,
)
from sources._socketcluster import IPOTAuthError, IPOTConnection, _describe_frame
from sources.ipot import IPOTSource

import argparse
import logging
import shlex
import threading
import time


LOGGER = logging.getLogger("ipot_service")

WIB = ZoneInfo("Asia/Jakarta")

CHECK_INTERVAL_SECONDS = 5 * 60

# No frame at all (not even a ping) for this long = treat the connection as dead
SILENCE_LIMIT_SECONDS = 90

# Anything else from the server gets logged
ROUTINE_EVENTS = ("notif", "record")

# A fixed past day always has data, so an empty answer means a bad session
HEARTBEAT_TICKER = "BBCA"
HEARTBEAT_DATE = "2026-10-13"

SCRAPE_AT = "18:00"
SCRAPE_RETRY_SECONDS = 60 * 60
SCRAPE_MAX_ATTEMPTS = 3


class SessionStats:
    def __init__(self):
        self.started_at = time.time()
        self.checks_ok = 0
        self.checks_failed = 0
        self.drops = 0
        self.reconnects_ok = 0
        self.reconnects_failed = 0
        self.scrapes_ok = 0
        self.longest_silence_seconds = 0.0

    def summary(self) -> str:
        hours_alive = (time.time() - self.started_at) / 3600

        return (
            f"ran {hours_alive:.2f}h | checks ok={self.checks_ok} failed={self.checks_failed} | "
            f"drops={self.drops} | reconnects ok={self.reconnects_ok} failed={self.reconnects_failed} | "
            f"scrapes ok={self.scrapes_ok} | "
            f"longest gap between frames={self.longest_silence_seconds:.0f}s"
        )


def heartbeat(connection: IPOTConnection) -> int:
    year, month, day = HEARTBEAT_DATE.split("-")
    ipot_date = f"{int(year)}-{int(month)}-{int(day)}"

    raw_records = connection.send_request(
        service="midata",
        cmd="query",
        param={
            "source": "datafeed",
            "index": "en_qu_top_bs",
            "args": ["b", HEARTBEAT_TICKER, "", "RG", "%", ipot_date, ipot_date],
        },
    )

    broker_rows = IPOTSource._parse_rows(
        ticker=HEARTBEAT_TICKER,
        date_str=HEARTBEAT_DATE,
        raw=raw_records,
    )

    return len(broker_rows)


def show_qrcode(qrcode_text: str, span_seconds: int) -> None:
    # The Dagu log pane can't draw a QR (no block-character font, fixed line
    # spacing), so log the text and draw it in a local terminal instead
    draw_command = (
        "uv run --with segno python -c "
        "\"import segno, sys; segno.make(sys.argv[1], error='h').terminal(compact=True)\" "
        + shlex.quote(qrcode_text)
    )

    LOGGER.info(
        "Scan within %ds. Draw the QR in a local terminal, then scan it with the IPOT app:\n%s",
        span_seconds,
        draw_command,
    )


def reconnect(
    old_connection: IPOTConnection | None,
    latest_token: str,
) -> IPOTConnection:
    """
    Open + handshake the new connection first, then close the old one, then
    log in immediately: the token can't be used while its session is open and
    only lives a few seconds after that session ends.
    """
    new_connection = IPOTConnection(autologintoken=latest_token)
    new_connection.connect()

    if old_connection is not None:
        old_connection.close()

    login_started_at = time.time()
    new_connection.authenticate()

    LOGGER.info(
        "RECONNECT OK: autologin %.2fs after old session closed",
        time.time() - login_started_at,
    )

    return new_connection


class SilenceWatchdog:
    """
    Closes the socket if no frame arrives for SILENCE_LIMIT_SECONDS.

    recv() blocks until a frame arrives, so a connection that dies silently
    would hang forever. Closing the socket from this thread makes the blocked
    recv() raise, which the main loop handles as a drop.
    """

    def __init__(self, stats: SessionStats):
        self.stats = stats
        self.connection: IPOTConnection | None = None
        self.last_frame_at = time.time()
        self.paused = False
        self.lock = threading.Lock()

        threading.Thread(target=self._watch, daemon=True).start()

    def watch(self, connection: IPOTConnection) -> None:
        with self.lock:
            self.connection = connection
            self.last_frame_at = time.time()

    def pause(self) -> None:
        with self.lock:
            self.paused = True

    def resume(self) -> None:
        with self.lock:
            self.paused = False
            self.last_frame_at = time.time()

    def frame_received(self) -> None:
        with self.lock:
            silence_seconds = time.time() - self.last_frame_at
            self.stats.longest_silence_seconds = max(self.stats.longest_silence_seconds, silence_seconds)
            self.last_frame_at = time.time()

    def _watch(self) -> None:
        while True:
            time.sleep(5)

            with self.lock:
                silence_seconds = time.time() - self.last_frame_at
                connection = self.connection
                paused = self.paused

            if paused or connection is None or connection.ws is None or silence_seconds < SILENCE_LIMIT_SECONDS:
                continue

            LOGGER.warning("SILENT CONNECTION: no frame for %.0fs, closing it to force a reconnect", silence_seconds)

            with self.lock:
                self.last_frame_at = time.time()

            try:
                connection.ws.close()
            except Exception:
                pass


def idle(
    connection: IPOTConnection,
    seconds: float,
    watchdog: SilenceWatchdog,
) -> None:
    """
    Keep reading frames so pings get answered (see _recv_obj), and log any
    unusual server event, e.g. a forced logout.
    """
    until = time.time() + seconds

    while time.time() < until:
        if connection.ws is None:
            raise ConnectionError("socket closed")

        message = connection._recv_obj()
        watchdog.frame_received()

        if message is None:
            continue

        event_name = message.get("event")

        if event_name not in ROUTINE_EVENTS:
            LOGGER.info("SERVER EVENT: %s", _describe_frame(message))


class ScrapeSchedule:
    def __init__(self):
        self.attempts: dict[str, int] = {}
        self.last_attempt_at: dict[str, float] = {}
        self.finished: set[str] = set()

    def due_date(self) -> date | None:
        now = datetime.now(WIB)
        hour, minute = (int(part) for part in SCRAPE_AT.split(":"))

        if (now.hour, now.minute) < (hour, minute):
            return None

        target = latest_trading_day(now.date())
        date_str = target.isoformat()

        if date_str in self.finished:
            return None

        if self.attempts.get(date_str, 0) >= SCRAPE_MAX_ATTEMPTS:
            return None

        if time.time() - self.last_attempt_at.get(date_str, 0) < SCRAPE_RETRY_SECONDS:
            return None

        return target


def already_uploaded(date_str: str) -> bool:
    # data/<date>.csv is gone after a restart, so ask the table instead
    response = (
        get_supabase()
        .table("idx_broker_summary_daily")
        .select("symbol")
        .eq("date", date_str)
        .limit(1)
        .execute()
    )

    return bool(response.data)


def scrape_day(
    target: date,
    connection: IPOTConnection,
    schedule: ScrapeSchedule,
    watchdog: SilenceWatchdog,
    stats: SessionStats,
) -> None:
    date_str = target.isoformat()

    schedule.attempts[date_str] = schedule.attempts.get(date_str, 0) + 1
    schedule.last_attempt_at[date_str] = time.time()

    try:
        if already_uploaded(date_str):
            LOGGER.info("SCRAPE SKIPPED: %s is already in Supabase", date_str)
            schedule.finished.add(date_str)
            return
    
    except Exception as error:
        LOGGER.warning(
            "SCRAPE: could not check Supabase for %s (%s: %s) | scraping anyway", 
            date_str, 
            type(error).__name__, 
            error
        )

    LOGGER.info(
        "SCRAPE START: %s (attempt %d/%d)", 
        date_str, 
        schedule.attempts[date_str], 
        SCRAPE_MAX_ATTEMPTS
    )

    args = argparse.Namespace(
        date=date_str,
        backfill=None,
        symbol=None,
        limit=None,
        delay=1.0,
        jitter=0.5,
        allow_empty=False,
    )

    # A second login would end this session, so IPOTSource reuses ours
    sources = (IPOTSource(connection=connection), None)

    # cmd_scrape reads frames itself, so the watchdog would see silence
    watchdog.pause()

    try:
        cmd_scrape(args, sources=sources)
        cmd_upload(argparse.Namespace(date=date_str))

    except SystemExit as exit_signal:
        # cmd_scrape exits 1 on an empty day exiting here would end the login
        LOGGER.warning(
            "SCRAPE EMPTY: %s produced no rows (exit %s). A holiday, or IPOT down. Retrying up to %d times.",
            date_str,
            exit_signal.code,
            SCRAPE_MAX_ATTEMPTS,
        )
        return

    except Exception as error:
        LOGGER.error("SCRAPE FAILED: %s: %s", type(error).__name__, error)
        return

    finally:
        watchdog.resume()

    schedule.finished.add(date_str)
    stats.scrapes_ok += 1
    LOGGER.info("SCRAPE DONE: %s | %s", date_str, stats.summary())

    csv_path = DATA_DIR / f"{date_str}.csv"
    csv_path.unlink(missing_ok=True)


def main() -> int:
    logging.getLogger("httpx").setLevel(logging.WARNING)

    stats = SessionStats()
    watchdog = SilenceWatchdog(stats=stats)
    schedule = ScrapeSchedule()

    connection = IPOTConnection(autologintoken="unused")
    connection.connect()

    connection.qr_login(show_qrcode=show_qrcode)

    LOGGER.info(
        "QR LOGIN OK at %s | scraping after %s WIB", 
        datetime.now(WIB).isoformat(timespec="seconds"), 
        SCRAPE_AT
    )

    watchdog.watch(connection)

    try:
        broker_count = heartbeat(connection)
        LOGGER.info("CHECK OK right after login: %d brokers", broker_count)
        stats.checks_ok += 1

        while True:
            try:
                idle(connection, CHECK_INTERVAL_SECONDS, watchdog)

                broker_count = heartbeat(connection)

                if broker_count > 0:
                    stats.checks_ok += 1
                    LOGGER.info("CHECK OK: %d brokers | %s", broker_count, stats.summary())
                else:
                    stats.checks_failed += 1
                    LOGGER.warning("CHECK EMPTY: query returned 0 brokers | %s", stats.summary())

                due = schedule.due_date()

                if due is not None:
                    scrape_day(due, connection, schedule, watchdog, stats)

            except KeyboardInterrupt:
                raise

            except Exception as error:
                stats.drops += 1
                LOGGER.warning("CONNECTION DROPPED: %s: %s", type(error).__name__, error)

                try:
                    # Close the old one first if it is still open (e.g. IPOT
                    # logged the session out but kept the socket): the token
                    # can't be used while its session is open
                    connection = reconnect(
                        old_connection=connection,
                        latest_token=connection.autologintoken,
                    )
                    stats.reconnects_ok += 1
                    watchdog.watch(connection)

                except Exception as reconnect_error:
                    stats.reconnects_failed += 1
                    LOGGER.error(
                        "RECONNECT FAILED: %s: %s | %s",
                        type(reconnect_error).__name__,
                        reconnect_error,
                        stats.summary(),
                    )
                    return 1

    except KeyboardInterrupt:
        LOGGER.info("Stopped by user")

    except IPOTAuthError as error:
        stats.reconnects_failed += 1
        LOGGER.error("AUTH FAILED: %s | %s", error, stats.summary())
        return 1

    finally:
        LOGGER.info("SUMMARY: %s", stats.summary())
        connection.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
