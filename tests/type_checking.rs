use trustworthiness_checker::{type_check_expr, ErrorType, SExpr, SExprT, SExprTE, StreamData};

#[tokio::test]
async fn test_int_val() {
    let val: SExpr<String> = SExpr::Val(StreamData::Int(1));
    let result = type_check_expr(val);
    let expected: Result<SExprTE<String>, ErrorType> = Ok(SExprTE::IntT(SExprT::Val(1)));

    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_string_val() {
    let val: SExpr<String> = SExpr::Val(StreamData::Str("Hello".into()));
    let result = type_check_expr(val);
    let expected: Result<SExprTE<String>, ErrorType> =
        Ok(SExprTE::StrT(SExprT::Val("Hello".into())));

    assert_eq!(result, expected);
}
