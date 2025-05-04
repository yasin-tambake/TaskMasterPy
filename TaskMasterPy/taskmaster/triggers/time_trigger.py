"""
Time-based triggers for TaskMasterPy.

This module defines triggers that fire based on time, such as at specific
intervals or at specific times of day.
"""
import threading
import time
from typing import Dict, Any, Optional
import schedule

from taskmaster.triggers.base import BaseTrigger


class TimeTrigger(BaseTrigger):
    """A trigger that fires based on time.
    
    This trigger can be configured to fire at specific intervals or at
    specific times of day.
    """
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new time trigger.
        
        Args:
            name: A unique name for this trigger
            config: Configuration parameters for the trigger
                - schedule_str: A string describing when to run the trigger
                  (e.g., "every 1 hour", "every day at 10:30")
        """
        super().__init__(name, config)
        self.schedule_str = self.config.get("schedule_str", "every 1 hour")
        self.thread: Optional[threading.Thread] = None
        self.job = None
    
    def activate(self) -> None:
        """Activate the trigger to start listening for time events."""
        super().activate()
        
        # Parse the schedule string and create the appropriate schedule
        schedule_parts = self.schedule_str.split()
        if len(schedule_parts) < 2:
            raise ValueError(f"Invalid schedule string: {self.schedule_str}")
        
        if schedule_parts[0].lower() == "every":
            # Handle "every X minutes/hours/days" format
            try:
                interval = int(schedule_parts[1])
                unit = schedule_parts[2].lower()
                
                job = None
                if unit in ["minute", "minutes"]:
                    job = schedule.every(interval).minutes
                elif unit in ["hour", "hours"]:
                    job = schedule.every(interval).hours
                elif unit in ["day", "days"]:
                    job = schedule.every(interval).days
                elif unit in ["week", "weeks"]:
                    job = schedule.every(interval).weeks
                else:
                    raise ValueError(f"Unsupported time unit: {unit}")
                
                # Handle "at HH:MM" suffix
                if len(schedule_parts) > 3 and schedule_parts[3].lower() == "at":
                    time_str = schedule_parts[4]
                    job = job.at(time_str)
                
                self.job = job.do(self._on_schedule)
                
            except (ValueError, IndexError) as e:
                raise ValueError(f"Invalid schedule string: {self.schedule_str}") from e
        else:
            raise ValueError(f"Unsupported schedule format: {self.schedule_str}")
        
        # Start the scheduler thread
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
    
    def deactivate(self) -> None:
        """Deactivate the trigger to stop listening for time events."""
        if self.job:
            schedule.cancel_job(self.job)
            self.job = None
        
        self.is_active = False
    
    def _on_schedule(self) -> None:
        """Called when the scheduled time is reached."""
        self.fire({"trigger_time": time.time()})
    
    def _run_scheduler(self) -> None:
        """Run the scheduler loop."""
        while self.is_active:
            schedule.run_pending()
            time.sleep(1)


class CronTrigger(BaseTrigger):
    """A trigger that fires based on a cron expression.
    
    This trigger uses the schedule library to implement cron-like scheduling.
    """
    
    def __init__(self, name: str = None, config: Dict[str, Any] = None):
        """Initialize a new cron trigger.
        
        Args:
            name: A unique name for this trigger
            config: Configuration parameters for the trigger
                - cron_expression: A cron-like expression (e.g., "0 * * * *")
        """
        super().__init__(name, config)
        self.cron_expression = self.config.get("cron_expression", "0 * * * *")
        self.thread: Optional[threading.Thread] = None
        self.job = None
    
    def activate(self) -> None:
        """Activate the trigger to start listening for cron events."""
        super().activate()
        
        # Parse the cron expression and create the appropriate schedule
        # Note: schedule doesn't support full cron expressions, so we'll
        # implement a simplified version
        
        parts = self.cron_expression.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {self.cron_expression}")
        
        minute, hour, day, month, day_of_week = parts
        
        # For simplicity, we'll only support specific values or '*'
        # A more complete implementation would handle ranges, lists, and steps
        
        job = schedule.every()
        
        # Set day of week (0-6, where 0 is Monday)
        if day_of_week != "*":
            days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
            try:
                day_index = int(day_of_week)
                if day_index < 0 or day_index > 6:
                    raise ValueError(f"Day of week must be 0-6, got {day_index}")
                job = getattr(job, days[day_index])
            except ValueError:
                raise ValueError(f"Invalid day of week: {day_of_week}")
        
        # Set day of month
        if day != "*":
            try:
                day_num = int(day)
                job = job.day(day_num)
            except ValueError:
                raise ValueError(f"Invalid day of month: {day}")
        
        # Set hour
        if hour != "*":
            try:
                hour_num = int(hour)
                job = job.at(f"{hour_num:02d}:00")
            except ValueError:
                raise ValueError(f"Invalid hour: {hour}")
        
        # Set minute
        if minute != "*":
            try:
                minute_num = int(minute)
                # If hour is specific, we need to adjust the time string
                if hour != "*":
                    hour_num = int(hour)
                    job = job.at(f"{hour_num:02d}:{minute_num:02d}")
                else:
                    # If hour is *, we can only specify the minute
                    job = job.minute(minute_num)
            except ValueError:
                raise ValueError(f"Invalid minute: {minute}")
        
        self.job = job.do(self._on_schedule)
        
        # Start the scheduler thread
        self.thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.thread.start()
    
    def deactivate(self) -> None:
        """Deactivate the trigger to stop listening for cron events."""
        if self.job:
            schedule.cancel_job(self.job)
            self.job = None
        
        self.is_active = False
    
    def _on_schedule(self) -> None:
        """Called when the scheduled time is reached."""
        self.fire({"trigger_time": time.time()})
    
    def _run_scheduler(self) -> None:
        """Run the scheduler loop."""
        while self.is_active:
            schedule.run_pending()
            time.sleep(1)
