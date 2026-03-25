"""
Root-level conftest.py for pytest.

Adds the project root to sys.path so that `src` and `redshift_compat` modules
can be imported without installing the package.
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))
