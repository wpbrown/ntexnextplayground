use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    task::{Context, Poll},
};

use futures_util::FutureExt;
use log::{error, info, trace, debug};
use ntex::{
    service::apply,
    task::LocalWaker,
    time::Millis,
    util::{buffer::Buffer, onerequest::OneRequest, poll_fn, BoxFuture, Ready},
    Container, Service, ServiceCtx, ServiceFactory,
};
use std::future::Future;

const TEST_SIZE: usize = 16;

#[ntex::main]
async fn main() {
    env_logger::init();
    let factory = TestServiceFactory::default();
    let control = factory.control();
    let factory = apply(OneRequest, factory);
    let factory = apply(Buffer::default().buf_size(6), factory);
    let dispatcher = ntex::rt::spawn(new_mock_dispatcher(factory).await);

    control.set_ready();
    info!("control is ready");
    ntex::time::sleep(Millis(22)).await;
    control.set_unready();
    info!("control is unready");
    ntex::time::sleep(Millis(500)).await;
    control.set_ready();
    info!("control is ready");

    let _ = dispatcher.await;
}

#[derive(Default)]
struct TestControl {
    ready: Rc<Cell<bool>>,
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
        if self.inputs.borrow().len() == TEST_SIZE {
            debug!("TestService fully processed: {:?}", self.inputs.borrow());
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
        async move {
            ntex::time::sleep(Millis(10)).await;

            self.inputs.borrow_mut().push(value);
            info!("TestService processed: {}", value);

            if self.inputs.borrow().len() == TEST_SIZE {
                self.shutdown_waker.wake();
            }

            Ok(())
        }
        .boxed_local()
    }
}

async fn new_mock_dispatcher<Sf: ServiceFactory<u32> + 'static>(
    factory: Sf,
) -> impl Future<Output = ()> {
    let srv = factory.create(()).await;

    mock_dispatcher(srv.unwrap_or_else(|_| unreachable!()))
}

async fn mock_dispatcher<S: Service<u32> + 'static>(srv: S) {
    let container_srv = Container::new(srv);

    for i in 0..16 {
        let _ = poll_fn(|cx| container_srv.poll_ready(cx)).await;
        mock_srv_call(&container_srv, i);
        ntex::time::sleep(Millis(10)).await;
    }

    info!("waiting for shutdown");
    let shutdown_fut = poll_fn(|cx| container_srv.poll_shutdown(cx));
    let _ = ntex::time::timeout(Millis::from_secs(3), shutdown_fut)
        .await
        .map_err(|()| error!("shutdown timeout"));
    info!("dispatcher is shutting down");
}

fn mock_srv_call<S: Service<u32> + 'static>(container: &Container<S>, value: u32) {
    let container = container.clone();
    ntex::rt::spawn(async move {
        // simulate degenerate wakeup order from the executor
        // ntex::time::sleep(Millis(128 - value)).await;

        trace!("Dispatch mock call with value: {}", value);
        let srv_call = container.call(value);
        trace!("Dispatched mock call with value: {}", value);
        let _ = srv_call.await;
        trace!("Mock response completed with value: {}", value);
    });
}

fn is_sorted<T>(data: &[T]) -> bool
where
    T: Ord,
{
    data.windows(2).all(|w| w[0] <= w[1])
}
