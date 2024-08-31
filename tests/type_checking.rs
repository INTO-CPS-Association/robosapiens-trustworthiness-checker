use trustworthiness_checker::{
    type_check_expr, ErrorType, SExpr, SExprInt, SExprStr, SExprT, StreamData,
};

#[tokio::test]
async fn test_int_val() {
    let val: SExpr<String> = SExpr::Val(StreamData::Int(1));
    let result = type_check_expr(val);
    let expected: Result<SExprT<String>, ErrorType> = Ok(SExprT::IntT(SExprInt::Val(1)));

    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_string_val() {
    let val: SExpr<String> = SExpr::Val(StreamData::Str("Hello".into()));
    let result = type_check_expr(val);
    let expected: Result<SExprT<String>, ErrorType> =
        Ok(SExprT::StrT(SExprStr::Val("Hello".into())));

    assert_eq!(result, expected);
}
