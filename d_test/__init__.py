from twisted.internet import protocol, reactor
from twisted.application import service
from twisted.python import log

from .og_updater_service import OgUpdater_UpdateLinksService



def start_application(\
        application, 
        observer,
        suggested_threadpool_size = 20, 
    ):
    
    ogUpdateLinksService= OgUpdater_UpdateLinksService()
    ogUpdateLinksService.setServiceParent(application)
    ogUpdateLinksService.startService()


    reactor.suggestThreadPoolSize(suggested_threadpool_size)
    reactor.run()
