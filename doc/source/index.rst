.. ga_irods documentation master file, created by
   sphinx-quickstart on Wed Mar 28 14:39:19 2012.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

ga_irods - Django+Celery -> iRODS bridge
========================================

There are a lot of buzzwords in that title; the gist of what this package does,
though, is allow you to write web-scale applications in `Django`_ that connect
to an `iRODS`_ data grid.  An iRODS data grid is a kind of distributed data
store that can be accessed via a specific set of commands similar in nature to
the standard unix filesystem tools, such as ls, rm, and so forth.  iRODS,
however, allows you to do several things you can't do with a regular filesystem
(without a little bit of magic, anyway):

* Write "rules" and "microservices" that allow you to manipulate data when it
  is stored, accessed, or at specific times.  
* Add arbitrary metadata to a file
* Expose the filesystem securely over the internet

This package allows you to access all the filesystem commands as Celery tasks
and gives you a base class, IRODSTask that allows you to create new tasks that
group together commands.

Contents:

.. toctree::
   :maxdepth: 2

   ga_irods

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

