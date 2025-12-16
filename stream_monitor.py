#!/usr/bin/env python3
"""
Stream Monitor - Fixed version with watchdog recovery
Fixes:
1. Watchdog thread that restarts dead monitor threads
2. Better error recovery and logging
3. Health check endpoint data
4. Periodic refresh of monitoring to catch any drift
"""
import time
import threading
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class StreamMonitor:
    """
    Monitors Twitch streams and automatically starts recordings when streamers go live.
    Includes watchdog to recover from thread death.
    """
    
    # Watchdog checks every 60 seconds for dead threads
    WATCHDOG_INTERVAL = 60
    
    # Force a full monitoring refresh every 4 hours
    FULL_REFRESH_INTERVAL = 4 * 60 * 60
    
    def __init__(self, app, db, recording_manager):
        self.app = app
        self.db = db
        self.recording_manager = recording_manager
        self._lock = threading.RLock()
        self._monitor_threads: Dict[int, threading.Thread] = {}
        self._stop_events: Dict[int, threading.Event] = {}
        self._running = True
        self._watchdog_thread: Optional[threading.Thread] = None
        self._last_full_refresh = time.time()
        self._thread_death_count: Dict[int, int] = {}  # streamer_id -> death count
        
        # Start the watchdog
        self._start_watchdog()
        
    def _start_watchdog(self):
        """Start the watchdog thread that monitors and restarts dead threads"""
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="monitor-watchdog",
            daemon=True
        )
        self._watchdog_thread.start()
        logger.info("Stream monitor watchdog started")
    
    def _watchdog_loop(self):
        """
        Watchdog loop that:
        1. Detects dead monitor threads and restarts them
        2. Periodically refreshes all monitoring to catch any drift
        """
        while self._running:
            try:
                time.sleep(self.WATCHDOG_INTERVAL)
                
                if not self._running:
                    break
                
                # Check for dead threads
                dead_streamers = self._check_for_dead_threads()
                
                # Restart dead threads
                for streamer_id in dead_streamers:
                    self._restart_monitor(streamer_id)
                
                # Periodic full refresh
                now = time.time()
                if now - self._last_full_refresh > self.FULL_REFRESH_INTERVAL:
                    logger.info("Performing periodic full monitoring refresh")
                    self._periodic_refresh()
                    self._last_full_refresh = now
                    
            except Exception as e:
                logger.exception(f"Watchdog loop error: {e}")
    
    def _check_for_dead_threads(self) -> list:
        """Check for monitor threads that have died and return their streamer IDs"""
        dead_streamers = []
        
        with self._lock:
            for streamer_id, thread in list(self._monitor_threads.items()):
                if not thread.is_alive():
                    logger.warning(f"Monitor thread for streamer {streamer_id} is dead!")
                    dead_streamers.append(streamer_id)
                    
                    # Track death count
                    self._thread_death_count[streamer_id] = self._thread_death_count.get(streamer_id, 0) + 1
                    
                    # Clean up dead thread tracking
                    self._monitor_threads.pop(streamer_id, None)
                    self._stop_events.pop(streamer_id, None)
        
        if dead_streamers:
            logger.warning(f"Found {len(dead_streamers)} dead monitor threads: {dead_streamers}")
            
        return dead_streamers
    
    def _restart_monitor(self, streamer_id: int):
        """Restart monitoring for a streamer whose thread died"""
        death_count = self._thread_death_count.get(streamer_id, 0)
        
        # Back off if thread keeps dying (exponential backoff capped at 5 minutes)
        backoff = min(300, 10 * (2 ** min(death_count - 1, 5)))
        
        if death_count > 1:
            logger.warning(f"Streamer {streamer_id} monitor has died {death_count} times, backing off {backoff}s")
            time.sleep(backoff)
        
        # Verify streamer is still active before restarting
        try:
            with self.app.app_context():
                Streamer = self.app.extensions.get('models', {}).get('Streamer')
                if Streamer:
                    streamer = Streamer.query.get(streamer_id)
                    if streamer and streamer.is_active:
                        logger.info(f"Restarting monitor for streamer {streamer_id} (death count: {death_count})")
                        self.start_monitoring(streamer_id)
                    else:
                        logger.info(f"Not restarting monitor for streamer {streamer_id} - inactive or deleted")
                        # Reset death count since we're not restarting
                        self._thread_death_count.pop(streamer_id, None)
        except Exception as e:
            logger.exception(f"Error restarting monitor for streamer {streamer_id}: {e}")
    
    def _periodic_refresh(self):
        """
        Periodically refresh all monitoring to ensure we haven't missed any streamers.
        This catches cases where streamers were added while app was running but
        monitoring wasn't started properly.
        """
        try:
            with self.app.app_context():
                Streamer = self.app.extensions.get('models', {}).get('Streamer')
                if Streamer is None:
                    return
                
                active_streamers = Streamer.query.filter_by(is_active=True).all()
                active_ids = {s.id for s in active_streamers}
                
                # Find streamers that should be monitored but aren't
                with self._lock:
                    currently_monitored = set(self._monitor_threads.keys())
                
                missing = active_ids - currently_monitored
                extra = currently_monitored - active_ids
                
                if missing:
                    logger.warning(f"Found {len(missing)} active streamers not being monitored: {missing}")
                    for streamer_id in missing:
                        self.start_monitoring(streamer_id)
                
                if extra:
                    logger.info(f"Found {len(extra)} monitored streamers that are no longer active: {extra}")
                    for streamer_id in extra:
                        self.stop_monitoring(streamer_id)
                        
                logger.info(f"Periodic refresh complete: {len(active_ids)} active streamers, {len(currently_monitored)} monitored")
                
        except Exception as e:
            logger.exception(f"Error in periodic refresh: {e}")
        
    def start_monitoring(self, streamer_id: int):
        """Start monitoring a specific streamer"""
        with self._lock:
            if streamer_id in self._monitor_threads:
                thread = self._monitor_threads[streamer_id]
                if thread.is_alive():
                    logger.debug(f"Streamer {streamer_id} already being monitored")
                    return
                else:
                    # Clean up dead thread
                    self._monitor_threads.pop(streamer_id, None)
                    self._stop_events.pop(streamer_id, None)
            
            stop_event = threading.Event()
            thread = threading.Thread(
                target=self._monitor_streamer_wrapper,
                args=(streamer_id, stop_event),
                name=f"monitor-{streamer_id}",
                daemon=True
            )
            
            self._monitor_threads[streamer_id] = thread
            self._stop_events[streamer_id] = stop_event
            thread.start()
            
            logger.info(f"Started monitoring streamer {streamer_id}")
    
    def stop_monitoring(self, streamer_id: int):
        """Stop monitoring a specific streamer"""
        with self._lock:
            stop_event = self._stop_events.get(streamer_id)
            if stop_event:
                stop_event.set()
            
            # Remove from tracking
            self._monitor_threads.pop(streamer_id, None)
            self._stop_events.pop(streamer_id, None)
            self._thread_death_count.pop(streamer_id, None)
            
            logger.info(f"Stopped monitoring streamer {streamer_id}")
    
    def start_monitoring_all_active(self) -> int:
        """Start monitoring all active streamers"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.app.app_context():
                    Streamer = self.app.extensions.get('models', {}).get('Streamer')
                    if Streamer is None:
                        raise RuntimeError("Streamer model not registered")
                    
                    active_streamers = Streamer.query.filter_by(is_active=True).all()
                    streamer_ids = [streamer.id for streamer in active_streamers]
                
                # Start monitoring outside the app context
                for streamer_id in streamer_ids:
                    self.start_monitoring(streamer_id)
                    
                count = len(streamer_ids)
                logger.info(f"Started monitoring {count} active streamers")
                return count
                    
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed to start monitoring: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
                else:
                    logger.exception(f"Error starting monitoring after {max_retries} attempts: {e}")
                    return 0
    
    def stop_all_monitoring(self):
        """Stop monitoring all streamers"""
        with self._lock:
            streamer_ids = list(self._monitor_threads.keys())
        
        for streamer_id in streamer_ids:
            self.stop_monitoring(streamer_id)
        
        logger.info("Stopped monitoring all streamers")
    
    def _monitor_streamer_wrapper(self, streamer_id: int, stop_event: threading.Event):
        """Wrapper that provides Flask app context to the monitor loop"""
        try:
            with self.app.app_context():
                self._monitor_streamer(streamer_id, stop_event)
        except Exception as e:
            logger.exception(f"Monitor wrapper crashed for streamer {streamer_id}: {e}")
        finally:
            # Ensure cleanup happens even if app_context fails
            with self._lock:
                self._monitor_threads.pop(streamer_id, None)
                self._stop_events.pop(streamer_id, None)
    
    def _monitor_streamer(self, streamer_id: int, stop_event: threading.Event):
        """Monitor a single streamer for live status"""
        logger.info(f"Starting monitor loop for streamer {streamer_id}")
        
        consecutive_errors = 0
        max_consecutive_errors = 10  # Give up after 10 consecutive errors
        
        while not stop_event.is_set() and self._running:
            try:
                models = self.app.extensions.get('models', {})
                Streamer = models.get('Streamer')
                AppConfig = models.get('AppConfig')
                from twitch_manager import TwitchManager, StreamStatus
                
                if Streamer is None or AppConfig is None:
                    raise RuntimeError("Models not registered")
                
                # Get streamer info
                streamer = Streamer.query.get(streamer_id)
                if not streamer or not streamer.is_active:
                    logger.info(f"Streamer {streamer_id} no longer active, stopping monitor")
                    break
                
                timer = streamer.timer or 360
                
                # Check if already recording
                if self.recording_manager.is_streamer_recording(streamer_id):
                    if stop_event.wait(timer):
                        break
                    continue
                
                # Check Twitch status
                try:
                    config = AppConfig(streamer)
                    twitch_manager = TwitchManager(config)
                    status, title = twitch_manager.check_user(streamer.twitch_name)
                    
                    # Reset error count on successful API call
                    consecutive_errors = 0
                    
                    logger.debug(f"Streamer {streamer.twitch_name} status: {status.name if status else 'None'}")
                    
                    if status == StreamStatus.ONLINE:
                        logger.info(f"Streamer {streamer.twitch_name} is live: {title}")
                        
                        if title and title.strip():
                            streamer.title = title.strip()
                            self.db.session.commit()
                        
                        recording_id = self.recording_manager.start_recording(streamer_id)
                        if recording_id:
                            logger.info(f"Started recording {recording_id} for {streamer.twitch_name}")
                        else:
                            logger.warning(f"Failed to start recording for {streamer.twitch_name}")
                    
                    elif status == StreamStatus.ERROR:
                        consecutive_errors += 1
                        logger.warning(f"Error checking {streamer.twitch_name} (consecutive: {consecutive_errors})")
                        
                        if consecutive_errors >= max_consecutive_errors:
                            logger.error(f"Too many consecutive errors for {streamer.twitch_name}, exiting monitor")
                            break
                    
                except Exception as e:
                    consecutive_errors += 1
                    logger.exception(f"Error checking Twitch status for streamer {streamer_id}: {e}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"Too many consecutive errors for streamer {streamer_id}, exiting monitor")
                        break
            
                # Wait for next check
                if stop_event.wait(timer):
                    break
                    
            except Exception as e:
                consecutive_errors += 1
                logger.exception(f"Error in monitor loop for streamer {streamer_id}: {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"Too many errors in monitor loop for {streamer_id}, exiting")
                    break
                    
                if stop_event.wait(60):
                    break
        
        logger.info(f"Monitor loop ended for streamer {streamer_id}")
    
    def get_monitoring_status(self) -> Dict[int, bool]:
        """Get monitoring status for all streamers"""
        with self._lock:
            return {
                streamer_id: thread.is_alive() 
                for streamer_id, thread in self._monitor_threads.items()
            }
    
    def get_health_status(self) -> dict:
        """Return health status for monitoring/debugging"""
        with self._lock:
            alive_count = sum(1 for t in self._monitor_threads.values() if t.is_alive())
            return {
                "total_monitors": len(self._monitor_threads),
                "alive_monitors": alive_count,
                "dead_monitors": len(self._monitor_threads) - alive_count,
                "watchdog_alive": self._watchdog_thread.is_alive() if self._watchdog_thread else False,
                "thread_death_counts": dict(self._thread_death_count),
                "seconds_since_refresh": time.time() - self._last_full_refresh,
            }
    
    def shutdown(self):
        """Shutdown the stream monitor"""
        logger.info("Shutting down stream monitor")
        self._running = False
        self.stop_all_monitoring()
        
        # Wait for threads to finish
        with self._lock:
            threads = list(self._monitor_threads.values())
        
        for thread in threads:
            if thread.is_alive():
                thread.join(timeout=5)


# Flask extensions pattern
from flask import current_app


def init_stream_monitor(app, db, recording_manager):
    """Initialize stream monitor using Flask extensions pattern"""
    if 'stream_monitor' not in app.extensions:
        app.extensions['stream_monitor'] = StreamMonitor(app, db, recording_manager)
    return app.extensions['stream_monitor']


def get_stream_monitor():
    """Get stream monitor from current app"""
    from flask import current_app
    if 'stream_monitor' not in current_app.extensions:
        from recording_manager import get_recording_manager
        recording_manager = get_recording_manager()
        init_stream_monitor(current_app, current_app.extensions['sqlalchemy'], recording_manager)
    return current_app.extensions['stream_monitor']
