from twisted.application import service
from twisted.python import log

import d_test

application = service.Application('Deferred Test')
observer = log.PythonLoggingObserver()
observer.start()

d_test.start_application(
	application,
	observer,
)
