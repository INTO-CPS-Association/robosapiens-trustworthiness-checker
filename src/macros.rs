// From: https://stackoverflow.com/questions/51121446/how-do-i-assert-an-enum-is-a-specific-variant-if-i-dont-care-about-its-fields
#[macro_export]
macro_rules! is_enum_variant {
    ($v:expr, $p:pat) => {
        if let $p = $v { true } else { false }
    };
}

#[cfg(test)]
mod tests {
    use test_log::test;

    #[test]
    fn example() {
        assert!(is_enum_variant!(Some(42), Some(_)));
    }
}
