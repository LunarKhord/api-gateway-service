import asyncio
import logging
from datetime import datetime
from typing import Callable, Any, Optional
from enum import Enum

logger = logging.getLogger(__name__)

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreakerOpenException(Exception):
    """Raised when circuit breaker is open"""
    pass

class CircuitBreaker:
    """
    Circuit Breaker pattern implementation for API Gateway.
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failure threshold exceeded, requests fail immediately  
    - HALF_OPEN: Testing if service recovered
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        expected_exception: Exception = Exception
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.success_count = 0
        
        logger.info(
            f"Circuit breaker initialized: threshold={failure_threshold}, "
            f"timeout={recovery_timeout}s"
        )
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function through circuit breaker.
        
        Args:
            func: Async function to execute
            *args, **kwargs: Arguments for the function
            
        Returns:
            Result from function
            
        Raises:
            CircuitBreakerOpenException: If circuit is open
            Exception: Any exception from the function
        """
        if self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
            else:
                logger.warning(
                    f"Circuit breaker is OPEN. Failing fast. "
                    f"Reset in {self._time_until_reset()}s"
                )
                raise CircuitBreakerOpenException("Circuit breaker is OPEN")
        
        # Execute function
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e
    
    def _on_success(self) -> None:
        """Handle successful call"""
        self.success_count += 1
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            logger.info(
                f"✅ Circuit breaker test successful. Closing circuit. "
                f"Success count: {self.success_count}"
            )
            self._transition_to_closed()
        elif self.state == CircuitBreakerState.CLOSED:
            if self.failure_count > 0:
                logger.info(
                    f"Circuit breaker recovered. Resetting failure count from {self.failure_count}"
                )
                self.failure_count = 0
    
    def _on_failure(self) -> None:
        """Handle failed call"""
        self.failure_count += 1
        self.last_failure_time = datetime.now()
        
        logger.warning(
            f"⚠️ Circuit breaker failure {self.failure_count}/{self.failure_threshold}. "
            f"State: {self.state}"
        )
        
        if self.state == CircuitBreakerState.HALF_OPEN:
            logger.error("Circuit breaker test failed. Reopening circuit.")
            self._transition_to_open()
        elif self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.failure_threshold:
                logger.error(
                    f"❌ Circuit breaker threshold exceeded ({self.failure_threshold} failures). "
                    f"Opening circuit for {self.recovery_timeout}s"
                )
                self._transition_to_open()
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset"""
        if self.last_failure_time is None:
            return False
        
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        return elapsed >= self.recovery_timeout
    
    def _time_until_reset(self) -> int:
        """Calculate seconds until reset attempt"""
        if self.last_failure_time is None:
            return 0
        
        elapsed = (datetime.now() - self.last_failure_time).total_seconds()
        remaining = max(0, self.recovery_timeout - elapsed)
        return int(remaining)
    
    def _transition_to_open(self) -> None:
        """Transition to OPEN state"""
        self.state = CircuitBreakerState.OPEN
        logger.error(
            f"🔴 Circuit breaker OPEN. Will attempt reset after {self.recovery_timeout}s"
        )
    
    def _transition_to_half_open(self) -> None:
        """Transition to HALF_OPEN state"""
        self.state = CircuitBreakerState.HALF_OPEN
        logger.info("🟡 Circuit breaker HALF_OPEN. Testing recovery.")
    
    def _transition_to_closed(self) -> None:
        """Transition to CLOSED state"""
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        logger.info("🟢 Circuit breaker CLOSED. Normal operation resumed")
    
    def get_state(self) -> dict:
        """Get current circuit breaker state"""
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "time_until_reset": self._time_until_reset() if self.state == CircuitBreakerState.OPEN else 0
        }