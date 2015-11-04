
from celery import Celery

app = Celery('tasks_drift',
             broker='amqp://',
             backend='amqp://',
             include=['tasks_drift'])

# Optional configuration, see the application user guide.
app.conf.update(
    CELERY_TASK_RESULT_EXPIRES=3600,
    CELERYD_TASK_SOFT_TIME_LIMIT=200,
    CELERYD_TASK_TIME_LIMIT=300,
)

if __name__ == '__main__':
    app.start()
