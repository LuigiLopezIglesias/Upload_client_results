#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Several util classes used in the rest of the project
"""

# Includes
import os

# ------------------------------------------------------------------------------------------------------------------- #


def create_dir(directory):

    """
    Creates a directory if it do not exists
    :param directory: The name of the directory
    """

    if not os.path.exists(directory):
        os.makedirs(directory)

