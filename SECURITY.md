# Security policy

## Reporting a vulnerability

Do not open a public GitHub issue for security vulnerabilities.

Email security reports to **kirankbs** via GitHub (use the private vulnerability reporting feature in the Security tab, or reach out through the profile contact).

Include:
- Description of the vulnerability
- Steps to reproduce
- Impact assessment

We will acknowledge receipt within 48 hours and work with you to resolve before any public disclosure.

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.2.x | Yes |
| < 0.2 | No |

Upgrade to the latest release for security fixes.

## Dependencies

This library depends on `pyspark` and `delta-spark`. Monitor their security advisories independently:
- [Apache Spark security](https://spark.apache.org/security.html)
- [Delta Lake releases](https://github.com/delta-io/delta/releases)
