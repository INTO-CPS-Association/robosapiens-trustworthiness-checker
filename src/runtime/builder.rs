use std::rc::Rc;

use smol::LocalExecutor;

use crate::{
    LOLASpecification, Monitor, Value,
    core::{AbstractMonitorBuilder, OutputHandler, Runnable, Runtime, Semantics, StreamData},
    dep_manage::interface::DependencyManager,
    lang::dynamic_lola::type_checker::{TypedLOLASpecification, type_check},
};

use super::{
    asynchronous::{AsyncMonitorBuilder, Context},
    constraints::runtime::ConstraintBasedMonitorBuilder,
};

use static_assertions::assert_obj_safe;

trait AnonymousMonitorBuilder<M, V: StreamData>: 'static {
    fn executor(
        self: Box<Self>,
        ex: Rc<LocalExecutor<'static>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn model(self: Box<Self>, model: M) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    // fn input(self, input: Box<dyn InputProvider<Val = V>>) -> Self;
    fn input(
        self: Box<Self>,
        input: Box<dyn crate::InputProvider<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn output(
        self: Box<Self>,
        output: Box<dyn OutputHandler<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn dependencies(
        self: Box<Self>,
        dependencies: DependencyManager,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>>;

    fn build(self: Box<Self>) -> Box<dyn Runnable>;
}

assert_obj_safe!(AnonymousMonitorBuilder<(), ()>);

impl<
    M,
    V: StreamData,
    Mon: Runnable + 'static,
    MonBuilder: AbstractMonitorBuilder<M, V, Mon = Mon> + 'static,
> AnonymousMonitorBuilder<M, V> for MonBuilder
{
    fn executor(
        self: Box<Self>,
        ex: Rc<LocalExecutor<'static>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::executor(*self, ex))
    }

    fn model(self: Box<Self>, model: M) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::model(*self, model))
    }

    fn input(
        self: Box<Self>,
        input: Box<dyn crate::InputProvider<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::input(*self, input))
    }

    fn output(
        self: Box<Self>,
        output: Box<dyn OutputHandler<Val = V>>,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::output(*self, output))
    }

    fn dependencies(
        self: Box<Self>,
        dependencies: DependencyManager,
    ) -> Box<dyn AnonymousMonitorBuilder<M, V>> {
        Box::new(MonBuilder::dependencies(*self, dependencies))
    }

    fn build(self: Box<Self>) -> Box<dyn Runnable> {
        Box::new(MonBuilder::build(*self))
    }
}

struct TypeCheckingBuilder<Builder>(Builder);

impl<
    V: StreamData,
    Mon: Monitor<TypedLOLASpecification, V> + 'static,
    MonBuilder: AbstractMonitorBuilder<TypedLOLASpecification, V, Mon = Mon> + 'static,
> AbstractMonitorBuilder<LOLASpecification, V> for TypeCheckingBuilder<MonBuilder>
{
    type Mon = Mon;

    fn new() -> Self {
        Self(MonBuilder::new())
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self(self.0.executor(ex))
    }

    fn model(self, model: LOLASpecification) -> Self {
        let model = type_check(model).expect("Model failed to type check");
        Self(self.0.model(model))
    }

    fn input(self, input: Box<dyn crate::InputProvider<Val = V>>) -> Self {
        Self(self.0.input(input))
    }

    fn output(self, output: Box<dyn OutputHandler<Val = V>>) -> Self {
        Self(self.0.output(output))
    }

    fn dependencies(self, dependencies: DependencyManager) -> Self {
        Self(self.0.dependencies(dependencies))
    }

    fn build(self) -> Self::Mon {
        let builder = self.0.build();
        // Perform type checking here
        builder
    }
}

pub struct GenericMonitorBuilder<M, V: StreamData> {
    executor: Option<Rc<LocalExecutor<'static>>>,
    model: Option<M>,
    input: Option<Box<dyn crate::InputProvider<Val = V>>>,
    output: Option<Box<dyn OutputHandler<Val = V>>>,
    dependencies: Option<DependencyManager>,
    runtime: Runtime,
    semantics: Semantics,
}

impl<M, V: StreamData> GenericMonitorBuilder<M, V> {
    pub fn runtime(self, runtime: Runtime) -> Self {
        Self { runtime, ..self }
    }

    pub fn maybe_runtime(self, runtime: Option<Runtime>) -> Self {
        match runtime {
            Some(runtime) => self.runtime(runtime),
            None => self,
        }
    }

    pub fn semantics(self, semantics: Semantics) -> Self {
        Self { semantics, ..self }
    }

    pub fn maybe_semantics(self, semantics: Option<Semantics>) -> Self {
        match semantics {
            Some(semantics) => self.semantics(semantics),
            None => self,
        }
    }
}

impl AbstractMonitorBuilder<LOLASpecification, Value>
    for GenericMonitorBuilder<LOLASpecification, Value>
{
    type Mon = Box<dyn Runnable>;

    fn new() -> Self {
        Self {
            executor: None,
            model: None,
            input: None,
            output: None,
            dependencies: None,
            runtime: Runtime::Async,
            semantics: Semantics::Untimed,
        }
    }

    fn executor(self, ex: Rc<LocalExecutor<'static>>) -> Self {
        Self {
            executor: Some(ex),
            ..self
        }
    }

    fn model(self, model: LOLASpecification) -> Self {
        Self {
            model: Some(model),
            ..self
        }
    }

    fn input(self, input: Box<dyn crate::InputProvider<Val = Value>>) -> Self {
        Self {
            input: Some(input),
            ..self
        }
    }

    fn output(self, output: Box<dyn OutputHandler<Val = Value>>) -> Self {
        Self {
            output: Some(output),
            ..self
        }
    }

    fn dependencies(self, dependencies: DependencyManager) -> Self {
        Self {
            dependencies: Some(dependencies),
            ..self
        }
    }

    fn build(self) -> Self::Mon {
        let builder: Box<dyn AnonymousMonitorBuilder<LOLASpecification, Value>> =
            match (self.runtime, self.semantics) {
                (Runtime::Async, Semantics::Untimed) => Box::new(AsyncMonitorBuilder::<
                    LOLASpecification,
                    Context<Value>,
                    Value,
                    _,
                    crate::semantics::UntimedLolaSemantics,
                >::new()),
                (Runtime::Async, Semantics::TypedUntimed) => {
                    Box::new(TypeCheckingBuilder(AsyncMonitorBuilder::<
                        TypedLOLASpecification,
                        Context<Value>,
                        Value,
                        _,
                        crate::semantics::TypedUntimedLolaSemantics,
                    >::new()))
                }
                (Runtime::Constraints, Semantics::Untimed) => {
                    Box::new(ConstraintBasedMonitorBuilder::new())
                }
                // (Runtime::Distributed, Semantics::Untimed) => {
                //     Box::new(AsyncMonitorBuilder::<
                //         M,
                //         Context<V>,
                //         V,
                //         _,
                //         crate::semantics::UntimedLolaSemantics,
                //     >::new())
                // }
                _ => {
                    panic!("Unsupported runtime and semantics combination");
                }
            };

        let builder = match self.executor {
            Some(ex) => builder.executor(ex),
            None => builder,
        };
        let builder = match self.dependencies {
            Some(dependencies) => builder.dependencies(dependencies),
            None => builder,
        };
        let builder = match self.model {
            Some(model) => builder.model(model),
            None => builder,
        };
        let builder = if let Some(output) = self.output {
            builder.output(output)
        } else {
            builder
        };
        let builder = if let Some(input) = self.input {
            builder.input(input)
        } else {
            builder
        };

        builder.build()
    }
}
