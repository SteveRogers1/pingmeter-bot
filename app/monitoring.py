import time
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio

@dataclass
class PerformanceMetrics:
    """Метрики производительности"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    average_response_time: float = 0.0
    total_response_time: float = 0.0
    start_time: datetime = field(default_factory=datetime.now)
    
    def add_request(self, response_time: float, success: bool = True):
        """Добавляет метрику запроса"""
        self.total_requests += 1
        self.total_response_time += response_time
        
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        
        self.average_response_time = self.total_response_time / self.total_requests
    
    def get_success_rate(self) -> float:
        """Возвращает процент успешных запросов"""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
    
    def get_uptime(self) -> timedelta:
        """Возвращает время работы"""
        return datetime.now() - self.start_time

@dataclass
class ErrorLog:
    """Лог ошибок"""
    timestamp: datetime
    error_type: str
    error_message: str
    user_id: Optional[int] = None
    chat_id: Optional[int] = None
    stack_trace: Optional[str] = None

class Monitoring:
    """Система мониторинга"""
    
    def __init__(self):
        self.metrics = PerformanceMetrics()
        self.errors: List[ErrorLog] = []
        self.max_errors = 1000  # Максимальное количество ошибок в памяти
        self.db_metrics = PerformanceMetrics()
        self.api_metrics = PerformanceMetrics()
    
    def log_request(self, response_time: float, success: bool = True, 
                   request_type: str = "general"):
        """Логирует запрос"""
        self.metrics.add_request(response_time, success)
        
        if request_type == "database":
            self.db_metrics.add_request(response_time, success)
        elif request_type == "api":
            self.api_metrics.add_request(response_time, success)
    
    def log_error(self, error_type: str, error_message: str, 
                  user_id: Optional[int] = None, chat_id: Optional[int] = None,
                  stack_trace: Optional[str] = None):
        """Логирует ошибку"""
        error = ErrorLog(
            timestamp=datetime.now(),
            error_type=error_type,
            error_message=error_message,
            user_id=user_id,
            chat_id=chat_id,
            stack_trace=stack_trace
        )
        
        self.errors.append(error)
        
        # Ограничиваем количество ошибок в памяти
        if len(self.errors) > self.max_errors:
            self.errors = self.errors[-self.max_errors:]
        
        logging.error(f"Ошибка [{error_type}]: {error_message}")
    
    def get_recent_errors(self, minutes: int = 60) -> List[ErrorLog]:
        """Возвращает ошибки за последние минуты"""
        cutoff = datetime.now() - timedelta(minutes=minutes)
        return [error for error in self.errors if error.timestamp > cutoff]
    
    def get_error_summary(self, minutes: int = 60) -> Dict[str, int]:
        """Возвращает сводку ошибок по типам"""
        recent_errors = self.get_recent_errors(minutes)
        summary = {}
        
        for error in recent_errors:
            summary[error.error_type] = summary.get(error.error_type, 0) + 1
        
        return summary
    
    def get_health_status(self) -> Dict[str, any]:
        """Возвращает статус здоровья системы"""
        return {
            "uptime": str(self.metrics.get_uptime()),
            "total_requests": self.metrics.total_requests,
            "success_rate": f"{self.metrics.get_success_rate():.2f}%",
            "average_response_time": f"{self.metrics.average_response_time:.3f}s",
            "recent_errors": len(self.get_recent_errors(5)),  # За последние 5 минут
            "database_success_rate": f"{self.db_metrics.get_success_rate():.2f}%",
            "api_success_rate": f"{self.api_metrics.get_success_rate():.2f}%"
        }
    
    def log_performance(self):
        """Логирует метрики производительности"""
        health = self.get_health_status()
        logging.info(f"📊 Метрики производительности: {health}")

# Глобальный экземпляр мониторинга
monitoring = Monitoring()

def monitor_request(request_type: str = "general"):
    """Декоратор для мониторинга запросов"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            start_time = time.time()
            success = True
            
            try:
                result = await func(*args, **kwargs)
                return result
            except Exception as e:
                success = False
                monitoring.log_error(
                    error_type=type(e).__name__,
                    error_message=str(e),
                    stack_trace=str(e)
                )
                raise
            finally:
                response_time = time.time() - start_time
                monitoring.log_request(response_time, success, request_type)
        
        return wrapper
    return decorator

async def periodic_monitoring():
    """Периодический мониторинг системы"""
    while True:
        try:
            monitoring.log_performance()
            await asyncio.sleep(300)  # Каждые 5 минут
        except Exception as e:
            logging.error(f"Ошибка периодического мониторинга: {e}")
            await asyncio.sleep(60)  # При ошибке ждем минуту
