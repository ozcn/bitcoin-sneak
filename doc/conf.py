#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from bitcoin_sneak import __author__, __version__, __package_name__

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.todo',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinxcontrib.asyncio',
]

templates_path = ['_templates']

source_suffix = ['.rst', '.md']

master_doc = 'index'

project = __package_name__
copyright = '2016, ' + __author__
author = __author__

version = __version__
release = __version__

language = 'ja'

exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

pygments_style = 'sphinx'

todo_include_todos = True

html_theme = 'sphinx_rtd_theme'

html_static_path = ['_static']

htmlhelp_basename = __package_name__ + 'doc'

latex_elements = {
}

latex_documents = [
    (master_doc, __package_name__ + '.tex',
     __package_name__ + ' Documentation',
     __author__, 'manual'),
]

man_pages = [
    (master_doc, __package_name__, __package_name__ + ' Documentation',
     [author], 1)
]

texinfo_documents = [
    (master_doc, __package_name__, __package_name__ + ' Documentation',
     author, __package_name__, 'One line description of project.',
     'Miscellaneous'),
]

epub_title = project
epub_author = author
epub_publisher = author
epub_copyright = copyright

epub_exclude_files = ['search.html']
