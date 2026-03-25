"""Redshift SQL compatibility layer for PostgreSQL."""

from redshift_compat import convert_redshift_to_postgres, apply_template_substitution

__all__ = ["convert_redshift_to_postgres", "apply_template_substitution"]
