use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    task::{Context, Poll},
};

use futures_util::FutureExt;
use log::{debug, error, info, trace};
use ntex::{
    channel::condition::Condition,
    service::apply,
    task::LocalWaker,
    time::Millis,
    util::{buffer::Buffer, onerequest::OneRequest, poll_fn, BoxFuture, Ready},
    Pipeline, Service, ServiceCtx, ServiceFactory,
};
use std::future::Future;

const TEST_SIZE: u32 = 16;
const SIMULATE_DEGENERATE_WAKEUP: bool = true;
const USE_STATIC_CALL: bool = true;
const WAIT_FOR_INFLIGHT_BEFORE_SHUTDOWN: bool = false;
const TIME_TO_PROCESS_REQUEST: u32 = 500;
const BUFFER_SIZE: usize = 4;
const CANCEL_ON_SHUTDOWN: bool = false;

#[ntex::main]
async fn main() {
    env_logger::init();
    let factory = TestServiceFactory::default();
    let control = factory.control();
    let factory = apply(OneRequest, factory);
    let dispatcher = if CANCEL_ON_SHUTDOWN {
        control.set_cancelled();
        let factory = apply(Buffer::default().buf_size(4).cancel_on_shutdown(), factory);
        ntex::rt::spawn(new_mock_dispatcher(factory).await)
    } else {
        let factory = apply(Buffer::default().buf_size(BUFFER_SIZE), factory);
        ntex::rt::spawn(new_mock_dispatcher(factory).await)
    };

    control.set_ready();
    info!("control is ready");
    ntex::time::sleep(Millis(22)).await;
    control.set_unready();
    info!("control is unready");
    ntex::time::sleep(Millis(500)).await;
    control.set_ready();
    info!("control is ready");

    let _ = dispatcher.await;
    
    info!("test shutting down");
    ntex::time::sleep(Millis(1000)).await;
}

#[derive(Default)]
struct TestControl {
    ready: Cell<bool>,
    cancelled: Cell<bool>,
    waker: LocalWaker,
}

impl TestControl {
    fn set_ready(&self) {
        self.ready.set(true);
        self.waker.wake();
    }

    fn set_unready(&self) {
        self.ready.set(false);
    }

    fn set_cancelled(&self) {
        self.cancelled.set(true);
    }
}

struct TestService {
    control: Rc<TestControl>,
    inputs: RefCell<Vec<u32>>,
    shutdown_waker: LocalWaker,
}

#[derive(Default)]
struct TestServiceFactory {
    control: Rc<TestControl>,
}

impl TestServiceFactory {
    fn control(&self) -> Rc<TestControl> {
        Rc::clone(&self.control)
    }
}

impl ServiceFactory<u32, ()> for TestServiceFactory {
    type Response = ();
    type Error = ();
    type Service = TestService;
    type InitError = ();
    type Future<'f> = Ready<Self::Service, ()> where Self: 'f;

    fn create(&self, _: ()) -> Self::Future<'_> {
        Ready::Ok(TestService {
            control: self.control(),
            inputs: Default::default(),
            shutdown_waker: Default::default(),
        })
    }
}

impl Service<u32> for TestService {
    type Response = ();
    type Error = ();
    type Future<'f> = BoxFuture<'f, Result<Self::Response, Self::Error>> where Self: 'f;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.control.ready.get() {
            Poll::Ready(Ok(()))
        } else {
            self.control.waker.register(cx.waker());
            Poll::Pending
        }
    }

    fn poll_shutdown(&self, _cx: &mut std::task::Context<'_>) -> Poll<()> {
        if self.inputs.borrow().len() as u32 == TEST_SIZE || self.control.cancelled.get() {
            debug!(
                "TestService fully processed: {:?} (cancelled: {})",
                self.inputs.borrow(),
                self.control.cancelled.get()
            );
            if !is_sorted(&self.inputs.borrow()) {
                error!("TestService inputs are not in order");
            } else {
                info!("TestService inputs are in order");
            }
            Poll::Ready(())
        } else {
            self.shutdown_waker.register(_cx.waker());
            Poll::Pending
        }
    }

    fn call<'a>(&'a self, value: u32, _: ServiceCtx<'a, Self>) -> Self::Future<'a> {
        debug!("TestService called with: {}", value);
        async move {
            trace!("TestService future started with: {}", value);
            ntex::time::sleep(Millis(TIME_TO_PROCESS_REQUEST)).await;

            self.inputs.borrow_mut().push(value);
            info!("TestService processed: {}", value);

            if self.inputs.borrow().len() as u32 == TEST_SIZE {
                self.shutdown_waker.wake();
            }

            Ok(())
        }
        .boxed_local()
    }
}

#[derive(Default)]
struct DispatcherState {
    complete: Condition,
    inflight: Rc<Cell<u32>>,
}

async fn new_mock_dispatcher<Sf: ServiceFactory<u32> + 'static>(
    factory: Sf,
) -> impl Future<Output = ()> {
    let srv = factory.create(()).await;

    mock_dispatcher(srv.unwrap_or_else(|_| unreachable!()))
}

async fn mock_dispatcher<S: Service<u32> + 'static>(srv: S) {
    let state = DispatcherState::default();
    let container_srv = Pipeline::new(srv);

    for i in 0..TEST_SIZE {
        trace!("wait for service readiness");
        let _ = poll_fn(|cx| container_srv.poll_ready(cx)).await;
        trace!("service readiness is ready");
        if USE_STATIC_CALL {
            mock_srv_call_in_dispatch(
                container_srv.clone(),
                i,
                state.complete.clone(),
                state.inflight.clone(),
            );
        } else {
            mock_srv_call_in_spawn(
                container_srv.clone(),
                i,
                state.complete.clone(),
                state.inflight.clone(),
            );
        }
        ntex::time::sleep(Millis(10)).await;
    }

    info!("wait for inflight: {}", state.inflight.get());
    if WAIT_FOR_INFLIGHT_BEFORE_SHUTDOWN {
        loop {
            if state.inflight.get() == 0 {
                break;
            }
            trace!("* wait for srv ready: {}", state.inflight.get());
            let _ = poll_fn(|cx| container_srv.poll_ready(cx)).await;

            if state.inflight.get() == 0 {
                break;
            }
            trace!("* wait for task complete: {}", state.inflight.get());
            state.complete.wait().await;
        }
    }

    info!("waiting for shutdown");
    let shutdown_fut = poll_fn(|cx| container_srv.poll_shutdown(cx));
    let _ = ntex::time::timeout(Millis::from_secs(15), shutdown_fut)
        .await
        .map_err(|()| error!("shutdown timeout"));
    info!("dispatcher is shutting down");
}

fn mock_srv_call_in_dispatch<S: Service<u32> + 'static>(
    container: Pipeline<S>,
    value: u32,
    complete: Condition,
    inflight: Rc<Cell<u32>>,
) {
    inflight.set(inflight.get() + 1);
    trace!("Dispatch mock static_call with value: {}", value);
    let srv_call = container.call(value);
    trace!("Dispatched mock static_call with value: {}", value);
    ntex::rt::spawn(async move {
        if SIMULATE_DEGENERATE_WAKEUP {
            degenerate_sleep(value).await;
        }

        trace!("Waiting for mock response future with value: {}", value);
        let _ = srv_call.await;
        trace!("Mock response completed with value: {}", value);
        inflight.set(inflight.get() - 1);
        complete.notify();
    });
}

fn mock_srv_call_in_spawn<S: Service<u32> + 'static>(
    container: Pipeline<S>,
    value: u32,
    complete: Condition,
    inflight: Rc<Cell<u32>>,
) {
    inflight.set(inflight.get() + 1);
    ntex::rt::spawn(async move {
        if SIMULATE_DEGENERATE_WAKEUP {
            degenerate_sleep(value).await;
        }

        trace!("Dispatch mock call with value: {}", value);
        let srv_call = container.call(value);
        trace!("Dispatched mock call with value: {}", value);

        trace!("Waiting for mock response future with value: {}", value);
        let res = srv_call.await;
        trace!(
            "Mock response completed with value: {}, is_err: {}",
            value,
            res.is_err()
        );
        inflight.set(inflight.get() - 1);
        complete.notify();
    });
}

async fn degenerate_sleep(value: u32) {
    const SLEEP_INCREMENT: u32 = 5;
    const MAX_SLEEP: u32 = (TEST_SIZE + 1) * SLEEP_INCREMENT;
    ntex::time::sleep(Millis(MAX_SLEEP - value * SLEEP_INCREMENT)).await;
}

fn is_sorted<T>(data: &[T]) -> bool
where
    T: Ord,
{
    data.windows(2).all(|w| w[0] <= w[1])
}
