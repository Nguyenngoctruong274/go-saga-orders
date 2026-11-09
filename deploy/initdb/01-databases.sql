-- Each daemon owns a database of its own; they share nothing but Kafka.
-- The *_test databases back the integration tests (`make test-db`, from day 4).
CREATE DATABASE inventory;
CREATE DATABASE payment;
CREATE DATABASE orders_test;
CREATE DATABASE inventory_test;
CREATE DATABASE payment_test;
