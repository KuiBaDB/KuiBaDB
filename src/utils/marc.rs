pub trait Destory {
    type DestoryCtx;
    fn destory(&mut self, ctx: &Self::DestoryCtx);
}
