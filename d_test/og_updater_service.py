import logging
log = logging.getLogger(__name__)

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

from twisted.application import internet
from twisted.python import threadable
from twisted.internet import defer
from twisted.internet import reactor
from twisted.internet import task
from twisted.internet import threads
from twisted.enterprise import adbapi
from twisted.enterprise.adbapi import ConnectionPool

from . import constants
from . import database
from .og_updater_task import OgUpdateLinks
from .service_scaffold import RequestWrapper , ServiceScaffold


class _OgUpdateLinksRequestWrapper(RequestWrapper):

    def _T_thread_begin( self ):
        """ runs the wrapper via `runInteraction` 
            https://twistedmatrix.com/documents/current/api/twisted.enterprise.adbapi.ConnectionPool.html#runInteraction
            
            > Interact with the database and return the result.
            >     The 'interaction' is a callable object which will be executed in a thread using a pooled connection. It will be passed an Transaction object as an argument (whose interface is identical to that of the database cursor for your DB-API module of choice), and its results will be returned as a Deferred. If running the method raises an exception, the transaction will be rolled back. If the method returns a value, the transaction will be committed.
            >   NOTE that the function you pass is *not* run in the main thread: you may have to worry about thread-safety in the function you pass to this if it tries to use non-local objects.
        """
        log.debug("_OgUpdateLinksRequestWrapper[%s]._T_thread_begin" , id(self) )
        self.updater = OgUpdateLinks()
        log.debug("_OgUpdateLinksRequestWrapper[%s]._T_thread_begin || about to run interaction" , id(self) )
        d = self.dbConnectionPool.runInteraction( self.updater.action_for_data , self.queued_data )\
            .addErrback( self._T_errors )
        log.debug("_OgUpdateLinksRequestWrapper[%s]._T_thread_begin || did run interaction" , id(self) )
        if constants.RETURN_D__THREAD_BEGIN :
            return d


    def _T_errors( self , x ):
        log.debug("_OgUpdateLinksRequestWrapper[%s]._T_errors" , id(self) )
        log.debug("_OgUpdateLinksRequestWrapper[%s]._T_errors | failed on : %s" , id(self) , self.queued_data )
        raise x



class _OgUpdateLinksService(ServiceScaffold):
    SEMAPHORE_TOKENS = 5

    def init_ensured( self , rval=None ):
        log.debug("_OgUpdateLinksService[%s].init_ensured" , id(self) )
        self.updater= OgUpdateLinks()
        ServiceScaffold.init_ensured(self,rval)


    def get_batch( self , rval=None ):
        log.debug("_OgUpdateLinksService[%s].get_batch" , id(self) )
        queued_updates= []
        self.updater= OgUpdateLinks()
        d = database.get_dbPool()\
            .runInteraction( self.updater.get_update_batch , queued_updates )\
            .addCallback( self.process_batch )\
            .addErrback( self._action_error )
        self.d = d


    def process_batch( self , queued_updates ):
        log.debug("_OgUpdateLinksService[%s].process_batch", id(self) )
        log.debug("_OgUpdateLinksService[%s].process_batch - Got Updates - %s" , id(self) , len(queued_updates) )
        if len( queued_updates ):
            updates= []
            for item in queued_updates:
                requestWrapper = _OgUpdateLinksRequestWrapper(\
                    deferredSemaphoreService = self.deferredSemaphoreService ,
                    dbConnectionPool = database.get_dbPool()
                )
                result = requestWrapper.queue_thread( data=item )
                updates.append(result)
                
            log.debug("[%s] updates =>", id(self) )
            log.debug("[%s] %s", id(self) , updates)
            for i in updates:
                log.debug("[%s] * %s", id(self) , i)
            self.d_list = defer.DeferredList( updates )\
                .addCallback( self.deferred_list_finish )
            log.debug("[%s] d_list => %s", id(self) , self.d_list )
        else:
            self.d = defer.Deferred()
            self.deferred_list_finish( self.d )







OgUpdateLinksService= _OgUpdateLinksService()


class OgUpdater_UpdateLinksService(internet.TimerService):
    def __init__( self ):
        internet.TimerService.__init__( self,
            15 ,
            OgUpdateLinksService.service_start
        )

