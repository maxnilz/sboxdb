CREATE TABLE warehouse (
    w_id INTEGER PRIMARY KEY,          -- Warehouse ID
    w_name VARCHAR(10),
    w_street_1 VARCHAR(20),
    w_street_2 VARCHAR(20),
    w_city VARCHAR(20),
    w_state CHAR(2),
    w_zip CHAR(9),
    w_tax DECIMAL(4,4),
    w_ytd DECIMAL(12,2)
);

CREATE TABLE district (
    d_id INTEGER,                      -- District ID
    d_w_id INTEGER,                    -- REFERENCES warehouse(w_id),
    d_name VARCHAR(10),
    d_street_1 VARCHAR(20),
    d_street_2 VARCHAR(20),
    d_city VARCHAR(20),
    d_state CHAR(2),
    d_zip CHAR(9),
    d_tax DECIMAL(4,4),
    d_ytd DECIMAL(12,2),
    d_next_o_id INTEGER,               -- The next order ID for the district
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE customer (
    c_id INTEGER,                      -- Customer ID
    c_d_id INTEGER,                    -- District ID
    c_w_id INTEGER,                    -- Warehouse ID
    c_first VARCHAR(16),
    c_middle CHAR(2),                  -- By Convention, TPC-C always set to OE(Order Entry).
    c_last VARCHAR(16),
    c_street_1 VARCHAR(20),
    c_street_2 VARCHAR(20),
    c_city VARCHAR(20),
    c_state CHAR(2),
    c_zip CHAR(9),
    c_phone CHAR(16),
    c_since TIMESTAMP,                -- When the customer first registered with the warehouse/district
    c_credit CHAR(2),                 -- GC/BC, i.e., GoodCredit or BadCredit
    c_credit_lim DECIMAL(12,2),       -- Credit limit
    c_discount DECIMAL(4,4),
    c_balance DECIMAL(12,2),          -- Tracks how much the customer owes or has available, New order become negative, Payment become positive.
    c_ytd_payment DECIMAL(12,2),
    c_payment_cnt INTEGER,
    c_delivery_cnt INTEGER,
    c_data TEXT,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE history (
    h_c_id     INTEGER,               -- Customer ID (who made the payment)
    h_c_d_id   INTEGER,               -- Customer's District ID
    h_c_w_id   INTEGER,               -- Customer's Warehouse ID
    h_d_id     INTEGER,               -- District where payment was made
    h_w_id     INTEGER,               -- Warehouse where payment was made
    h_date     TIMESTAMP,             -- Date/time of the payment
    h_amount   DECIMAL(6,2),          -- Payment amount
    h_data     VARCHAR(24),           -- Free-text comment (warehouse/district names)
    PRIMARY KEY (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id)   -- A workaround, Standard TPC-C have no primary key on history.
);

CREATE TABLE new_order (
    no_o_id INTEGER,
    no_d_id INTEGER,
    no_w_id INTEGER,
    PRIMARY KEY (no_w_id , no_d_id , no_o_id)
);

CREATE TABLE "order" (
    o_id INTEGER,
    o_d_id INTEGER,
    o_w_id INTEGER,
    o_c_id INTEGER,                     -- Customer ID
    o_entry_d TIMESTAMP,                -- Order entry date
    o_carrier_id INTEGER,
    o_ol_cnt INTEGER,                   -- Number of order lines
    o_all_local CHAR(1),                -- Whether all items are local
    PRIMARY KEY (o_w_id , o_d_id , o_id)
);

CREATE TABLE order_line (
    ol_o_id INTEGER,                    -- Order ID
    ol_d_id INTEGER,
    ol_w_id INTEGER,
    ol_number INTEGER,                  -- Line number
    ol_i_id INTEGER,                    -- Item ID
    ol_supply_w_id INTEGER,
    ol_delivery_d TIMESTAMP,
    ol_quantity INTEGER,
    ol_amount DECIMAL(12,2),
    ol_dist_info VARCHAR(24),           -- District-specific stock info (from stock.s_dist_x)
    PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number)
);

CREATE TABLE item (
    i_id INTEGER PRIMARY KEY,           -- Item ID
    i_im_id INTEGER,
    i_name VARCHAR(24),
    i_price DECIMAL(5,2),
    i_data VARCHAR(50)                  -- ~10% of rows including the substring "original" to introduce variety.
);

CREATE TABLE stock (
    s_i_id INTEGER,                     -- Item ID
    s_w_id INTEGER,                     -- Warehouse ID
    s_quantity INTEGER,                 -- Available quantity
    s_dist_01 VARCHAR(24),
    s_dist_02 VARCHAR(24),
    s_dist_03 VARCHAR(24),
    s_dist_04 VARCHAR(24),
    s_dist_05 VARCHAR(24),
    s_dist_06 VARCHAR(24),
    s_dist_07 VARCHAR(24),
    s_dist_08 VARCHAR(24),
    s_dist_09 VARCHAR(24),
    s_dist_10 VARCHAR(24),
    s_ytd INTEGER,
    s_order_cnt INTEGER,
    s_remote_cnt INTEGER,
    s_data VARCHAR(50),              -- ~10% of rows contain the substring "original", Like i_data, this is used to determine if an order is flagged as an “original” order.
    PRIMARY KEY (s_w_id, s_i_id)
);
