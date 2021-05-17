.. role:: raw-html-m2r(raw)
   :format: html


datar
=====

Port of `dplyr <https://dplyr.tidyverse.org/index.html>`_ and other related R packages in python, using `pipda <https://github.com/pwwang/pipda>`_.

Unlike other similar packages in python that just mimic the piping sign, ``datar`` follows the API designs from the original packages as possible. So that nearly no extra effort is needed for those who are familar with those R packages to transition to python.

:raw-html-m2r:`<!-- badges -->`
`
.. image:: https://img.shields.io/pypi/v/datar?style=flat-square
   :target: https://img.shields.io/pypi/v/datar?style=flat-square
   :alt: Pypi
 <https://pypi.org/project/datar/>`_ `
.. image:: https://img.shields.io/github/v/tag/pwwang/datar?style=flat-square
   :target: https://img.shields.io/github/v/tag/pwwang/datar?style=flat-square
   :alt: Github
 <https://github.com/pwwang/datar>`_ 
.. image:: https://img.shields.io/github/workflow/status/pwwang/datar/Build%20and%20Deploy?style=flat-square
   :target: https://img.shields.io/github/workflow/status/pwwang/datar/Build%20and%20Deploy?style=flat-square
   :alt: Building
 `
.. image:: https://img.shields.io/github/workflow/status/pwwang/datar/Build%20Docs?label=Docs&style=flat-square
   :target: https://img.shields.io/github/workflow/status/pwwang/datar/Build%20Docs?label=Docs&style=flat-square
   :alt: Docs and API
 <https://pwwang.github.io/datar/>`_ `
.. image:: https://img.shields.io/codacy/grade/3d9bdff4d7a34bdfb9cd9e254184cb35?style=flat-square
   :target: https://img.shields.io/codacy/grade/3d9bdff4d7a34bdfb9cd9e254184cb35?style=flat-square
   :alt: Codacy
 <https://app.codacy.com/gh/pwwang/datar>`_ `
.. image:: https://img.shields.io/codacy/coverage/3d9bdff4d7a34bdfb9cd9e254184cb35?style=flat-square
   :target: https://img.shields.io/codacy/coverage/3d9bdff4d7a34bdfb9cd9e254184cb35?style=flat-square
   :alt: Codacy coverage
 <https://app.codacy.com/gh/pwwang/datar>`_

`Documentation <https://pwwang.github.io/datar/>`_ | `Reference Maps <https://pwwang.github.io/datar/reference-maps/ALL/>`_ | `Notebook Examples <https://pwwang.github.io/datar/notebooks/across/>`_ | `API <https://pwwang.github.io/datar/api/datar/>`_

Installtion
-----------

.. code-block:: shell

   pip install -U datar

``datar`` requires python 3.7.1+ and is backended by ``pandas (1.2+)``.

Example usage
-------------

.. code-block:: python

   from datar import f
   from datar.dplyr import mutate, filter, if_else
   from datar.tibble import tibble

   df = tibble(
       x=range(4),
       y=['zero', 'one', 'two', 'three']
   )
   df >> mutate(z=f.x)
   """# output
      x      y  z
   0  0   zero  0
   1  1    one  1
   2  2    two  2
   3  3  three  3
   """

   df >> mutate(z=if_else(f.x>1, 1, 0))
   """# output:
      x      y  z
   0  0   zero  0
   1  1    one  0
   2  2    two  1
   3  3  three  1
   """

   df >> filter(f.x>1)
   """# output:
      x      y
   0  2    two
   1  3  three
   """

   df >> mutate(z=if_else(f.x>1, 1, 0)) >> filter(f.z==1)
   """# output:
      x      y  z
   0  2    two  1
   1  3  three  1
   """

.. code-block:: python

   # works with plotnine
   import numpy
   from datar.base import sin, pi
   from plotnine import ggplot, aes, geom_line, theme_classic

   df = tibble(x=numpy.linspace(0, 2*pi, 500))
   (df >>
      mutate(y=sin(f.x), sign=if_else(f.y>=0, "positive", "negative")) >>
      ggplot(aes(x='x', y='y')) + theme_classic()
   ) + geom_line(aes(color='sign'), size=1.2)


.. image:: ./example.png
   :target: ./example.png
   :alt: example


.. code-block:: python

   # very easy to integrate with other libraries
   # for example: klib
   import klib
   from pipda import register_verb
   from datar.datasets import iris
   from datar.dplyr import pull

   dist_plot = register_verb(func=klib.dist_plot)
   iris >> pull(f.Sepal_Length) >> dist_plot()


.. image:: ./example2.png
   :target: ./example2.png
   :alt: example

