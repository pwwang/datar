import argparse


def test_getattr(module, allow_conflict_names, fun, error):
    from datar import options
    options(allow_conflict_names=allow_conflict_names)

    if module == "all":
        import datar.all as d
    elif module == "base":
        import datar.base as d
    elif module == "dplyr":
        import datar.dplyr as d

    if not error:
        return getattr(d, fun)

    try:
        getattr(d, fun)
    except Exception as e:
        raised = type(e).__name__
        assert raised == error, f"Raised {raised}, expected {error}"
    else:
        raise AssertionError(f"{error} should have raised")


def _import(module, fun):
    if module == "all" and fun == "sum":
        from datar.all import sum  # noqa: F401
    elif module == "all" and fun == "slice":
        from datar.all import slice  # noqa: F401
    elif module == "base" and fun == "sum":
        from datar.base import sum  # noqa: F401
    elif module == "dplyr" and fun == "slice":
        from datar.dplyr import slice  # noqa: F401


def test_import(module, allow_conflict_names, fun, error):
    from datar import options
    options(allow_conflict_names=allow_conflict_names)

    if not error:
        return _import(module, fun)

    try:
        _import(module, fun)
    except Exception as e:
        raised = type(e).__name__
        assert raised == error, f"Raised {raised}, expected {error}"
    else:
        raise AssertionError(f"{error} should have raised")


def make_test(module, allow_conflict_names, getattr, fun, error):
    if fun == "_":
        fun = "sum" if module in ["all", "base"] else "slice"

    if getattr:
        return test_getattr(module, allow_conflict_names, fun, error)

    return test_import(module, allow_conflict_names, fun, error)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--module",
        choices=["all", "base", "dplyr"],
        required=True,
        help="The module to test"
    )
    parser.add_argument(
        "--allow-conflict-names",
        action="store_true",
        help="Whether to allow conflict names",
        default=False,
    )
    parser.add_argument(
        "--getattr",
        action="store_true",
        help=(
            "Whether to test datar.all.sum, "
            "otherwise test from datar.all import sum."
        ),
        default=False,
    )
    parser.add_argument(
        "--fun",
        help=(
            "The function to test. "
            "If _ then sum for all/base, slice for dplyr"
        ),
        choices=["sum", "filter", "_"],
        default="_",
    )
    parser.add_argument(
        "--error",
        help="The error to expect",
    )
    args = parser.parse_args()

    make_test(
        args.module,
        args.allow_conflict_names,
        args.getattr,
        args.fun,
        args.error,
    )


if __name__ == "__main__":
    main()
