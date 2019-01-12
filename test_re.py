date_pattern = ".*\((20[0-9]+-[0-9]+-[0-9]+)\)"
date_str = 'January 5, 2018 (2018-01-05) (United States)'

import re

print(re.match(date_pattern, date_str).group(1))