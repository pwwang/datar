import pytest
from datar.base.bessel import (
    bessel_i,
    bessel_j,
    bessel_k,
    bessel_y,
)
from datar.base import NA, Inf
from ..conftest import assert_iterable_equal, is_installed

pytestmark = pytest.mark.skipif(
    not is_installed("scipy"), reason="'scipy' is not installed"
)


@pytest.mark.parametrize(
    "x,nu,exp",
    [
        ([0, 1, 2, 3, NA], 0, [1, 0.765198, 0.223891, -0.260052, NA]),
        ([0, 1, 2, 3, NA], 1, [0, 0.440051, 0.576725, 0.339059, NA]),
        ([0, 1, 2, 3, NA], 2, [0, 0.114903, 0.352834, 0.486091, NA]),
    ],
)
def test_bessel_j(x, nu, exp):
    out = bessel_j(x, nu)
    assert_iterable_equal(out, exp, approx=1e-5)


@pytest.mark.parametrize(
    "x,nu,exp",
    [
        ([0, 1, 2, 3, NA], 0, [-Inf, 0.088257, 0.510376, 0.376850, NA]),
        ([0, 1, 2, 3, NA], 1, [-Inf, -0.781213, -0.107032, 0.324674, NA]),
        ([0, 1, 2, 3, NA], 2, [-Inf, -1.650683, -0.617408, -0.160400, NA]),
    ],
)
def test_bessel_y(x, nu, exp):
    out = bessel_y(x, nu)
    assert_iterable_equal(out, exp, approx=1e-5)


@pytest.mark.parametrize(
    "x,nu,expon_scaled,exp",
    [
        (
            [0, 1, 2, 3, NA],
            0,
            True,
            [1, 0.46575961, 0.30850832, 0.2430004, NA],
        ),
        (
            [0, 1, 2, 3, NA],
            1,
            True,
            [0, 0.20791042, 0.21526929, 0.1968267, NA],
        ),
        (
            [0, 1, 2, 3, NA],
            2,
            True,
            [0, 0.04993878, 0.09323903, 0.1117825, NA],
        ),
        ([0, 1, 2, 3, NA], 0, False, [1, 1.2660659, 2.2795853, 4.880793, NA]),
        ([0, 1, 2, 3, NA], 1, False, [0, 0.5651591, 1.5906369, 3.953370, NA]),
        ([0, 1, 2, 3, NA], 2, False, [0, 0.1357477, 0.6889484, 2.245212, NA]),
    ],
)
def test_bessel_i(x, nu, expon_scaled, exp):
    out = bessel_i(x, nu, expon_scaled)
    assert_iterable_equal(out, exp, approx=1e-5)


@pytest.mark.parametrize(
    "x,nu,expon_scaled,exp",
    [
        (
            [0, 1, 2, 3, NA],
            0,
            True,
            [Inf, 1.14446307980689, 0.841568215070772, 0.697761598043852, NA],
        ),
        (
            [0, 1, 2, 3, NA],
            1,
            True,
            [Inf, 1.63615348626326, 1.03347684706869, 0.806563480128787, NA],
        ),
        (
            [0, 1, 2, 3, NA],
            2,
            True,
            [Inf, 4.41677005233341, 1.87504506213946, 1.23547058479638, NA],
        ),
        (
            [0, 1, 2, 3, NA],
            0,
            False,
            [
                Inf,
                0.421024438240708,
                0.113893872749533,
                0.0347395043862793,
                NA,
            ],
        ),
        (
            [0, 1, 2, 3, NA],
            1,
            False,
            [
                Inf,
                0.601907230197235,
                0.139865881816522,
                0.0401564311281942,
                NA,
            ],
        ),
        (
            [0, 1, 2, 3, NA],
            2,
            False,
            [Inf, 1.62483889863518, 0.253759754566056, 0.061510458471742, NA],
        ),
    ],
)
def test_bessel_k(x, nu, expon_scaled, exp):
    out = bessel_k(x, nu, expon_scaled)
    assert_iterable_equal(out, exp, approx=1e-5)
