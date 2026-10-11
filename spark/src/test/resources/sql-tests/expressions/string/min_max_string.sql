-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

statement
CREATE TABLE test_string_min_max(g INT, s STRING) USING parquet

-- U+FFFF sorts before supplementary characters in binary UTF-8 ordering.
-- The empty string is a value; groups with only NULLs have NULL extrema.
statement
INSERT INTO test_string_min_max VALUES
  (0, 'é'), (0, '中'), (0, '￿'), (0, '😀'), (0, '𐀀'),
  (1, ''), (1, NULL), (2, NULL), (2, NULL),
  (3, 'é'), (3, 'é'), (3, '東京')

query
SELECT min(s), max(s) FROM test_string_min_max

query
SELECT g, min(s), max(s) FROM test_string_min_max GROUP BY g

query
SELECT min(s), max(s) FROM test_string_min_max WHERE g = 2

query
SELECT min(s), max(s) FROM test_string_min_max WHERE g = -1

query
SELECT g, min(s), max(s) FROM test_string_min_max WHERE g = -1 GROUP BY g

query expect_fallback(datatype StringType is not supported)
SELECT g, s, min(s) OVER (PARTITION BY g), max(s) OVER (PARTITION BY g)
FROM test_string_min_max
