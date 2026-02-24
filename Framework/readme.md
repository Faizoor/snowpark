# ------------------------------------------------------------
# CONFIG STRUCTURE
#
# sanity_checks:
#   rules:
#     - check: <check_name>
#       severity: FAIL|WARN
#       objects:
#         - name: <object_name>
#           database: <optional>
#           schema: <optional>
#
# tables:
#   - name: <table_name>
#     database: <optional>
#     schema: <optional>
#     enabled: True|False
#     rules:
#       - check: <check_name>
#         columns: [...]
#         threshold: <optional>
#         severity: FAIL|WARN
# ------------------------------------------------------------