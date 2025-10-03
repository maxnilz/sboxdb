use chrono::Utc;
use log::debug;
use rand::rngs::SmallRng;
use rand::Rng;
use rand::SeedableRng;

use crate::error::Result;

pub const TPCC_TABLES: &str = include_str!("tables.sql");

struct Address {
    street1: String,
    street2: String,
    city: String,
    state: String,
    zip: String,
}

struct Records {
    table: String,
    header: Vec<String>,
    inner: Vec<Vec<String>>,
}
impl Records {
    fn new(table: impl Into<String>, header: Vec<impl Into<String>>) -> Self {
        Self {
            table: table.into(),
            header: header.into_iter().map(|it| it.into()).collect(),
            inner: vec![],
        }
    }

    fn push(&mut self, element: Vec<String>) {
        self.inner.push(element)
    }

    fn queries(self, sz: usize) -> Vec<String> {
        let mut output = vec![];
        let mut offset = 0;
        let mut q = String::new();
        for it in self.inner.into_iter() {
            if offset == 0 {
                q.push_str(
                    format!("INSERT INTO {} ({}) VALUES \n", self.table, self.header.join(", "))
                        .as_str(),
                );
            } else {
                q.push_str(",\n")
            }
            q.push_str(format!("({})", it.join(", ")).as_str());
            offset += 1;

            if offset == sz {
                q.push(';');
                output.push(q);
                offset = 0;
                q = String::new();
            }
        }
        if offset > 0 {
            q.push(';');
            output.push(q);
        }
        output
    }
}

macro_rules! i2a {
    ($val:expr) => {
        $val.to_string()
    };
}
macro_rules! io2a {
    ($val:expr) => {
        match $val {
            Some(v) => v.to_string(),
            None => "NULL".to_string(),
        }
    };
}
macro_rules! f2a2 {
    ($val: expr) => {
        format!("{:.2}", $val)
    };
}
macro_rules! f2a4 {
    ($val: expr) => {
        format!("{:.4}", $val)
    };
}
macro_rules! lit {
    ($val:expr) => {
        format!("'{}'", $val)
    };
}

macro_rules! olit {
    ($val:expr) => {
        match $val {
            Some(v) => lit!(v),
            None => "NULL".to_string(),
        }
    };
}

pub struct TpccGenData {
    warehouse: Records,
    district: Records,
    customizer: Records,
    history: Records,
    order: Records,
    order_line: Records,
    new_order: Records,
    item: Records,
    stock: Records,

    sz: usize,
}

impl TpccGenData {
    pub fn to_queries(self) -> Result<Vec<String>> {
        let sz = self.sz;
        let mut queries = vec![];
        queries.extend(self.warehouse.queries(sz));
        queries.extend(self.district.queries(sz));
        queries.extend(self.customizer.queries(sz));
        queries.extend(self.history.queries(sz));
        queries.extend(self.item.queries(sz));
        queries.extend(self.stock.queries(sz));
        queries.extend(self.order.queries(sz));
        queries.extend(self.order_line.queries(sz));
        queries.extend(self.new_order.queries(sz));
        Ok(queries)
    }
}

#[cfg(test)]
impl TpccGenData {
    pub fn to_csv(self, dir: &str) -> Result<()> {
        use std::fs;
        use std::path::Path;

        fs::create_dir_all(dir)?;

        // Write each table to its own CSV file
        fs::write(Path::new(dir).join("warehouse.csv"), Self::records_to_vec(self.warehouse)?)?;
        fs::write(Path::new(dir).join("district.csv"), Self::records_to_vec(self.district)?)?;
        fs::write(Path::new(dir).join("customer.csv"), Self::records_to_vec(self.customizer)?)?;
        fs::write(Path::new(dir).join("history.csv"), Self::records_to_vec(self.history)?)?;
        fs::write(Path::new(dir).join("order.csv"), Self::records_to_vec(self.order)?)?;
        fs::write(Path::new(dir).join("order_line.csv"), Self::records_to_vec(self.order_line)?)?;
        fs::write(Path::new(dir).join("new_order.csv"), Self::records_to_vec(self.new_order)?)?;
        fs::write(Path::new(dir).join("item.csv"), Self::records_to_vec(self.item)?)?;
        fs::write(Path::new(dir).join("stock.csv"), Self::records_to_vec(self.stock)?)?;

        Ok(())
    }

    fn records_to_vec(records: Records) -> Result<Vec<u8>> {
        use std::io::Write;
        let mut buff = vec![];
        for it in records.inner.iter() {
            buff.write(it.join(", ").as_bytes())?;
            buff.push('\n' as u8);
        }
        Ok(buff)
    }
}

pub struct TpccGenerator {
    num_warehouse: i32,

    num_items: i32,
    districts_per_warehouse: i32,

    customers_per_district: i32,
    orders_per_district: i32,

    rng: SmallRng,

    sz: usize,
}

impl TpccGenerator {
    pub fn new(num_warehouse: i32) -> Self {
        Self { num_warehouse, ..Self::default() }
    }

    pub fn generate(&mut self) -> Result<TpccGenData> {
        debug!("generating warehouses");
        let warehouse = self.generate_warehouses()?;
        debug!("generating districts");
        let district = self.generate_districts()?;
        debug!("generating customer and history");
        let (customizer, history) = self.generate_customer_and_history()?;
        debug!("generating items");
        let item = self.generate_items()?;
        debug!("generating stocks");
        let stock = self.generate_stock()?;
        debug!("generating orders");
        let (order, order_line, new_order) = self.generate_orders()?;
        Ok(TpccGenData {
            warehouse,
            district,
            customizer,
            history,
            item,
            stock,
            order,
            order_line,
            new_order,
            sz: self.sz,
        })
    }

    fn generate_warehouses(&mut self) -> Result<Records> {
        let header = vec![
            "w_id",
            "w_name",
            "w_street_1",
            "w_street_2",
            "w_city",
            "w_state",
            "w_zip",
            "w_tax",
            "w_ytd",
        ];
        let mut output = Records::new("warehouse", header);
        for w_id in 1..=self.num_warehouse {
            let w_name = self.random_alpha_string(6, 10);
            let w_addr = self.make_address();
            let w_tax = self.random_integer(10, 10) as f32 / 100.0;
            let w_ytd = 3_000_000.00_f32;
            let elements = vec![
                i2a!(w_id),
                lit!(w_name),
                lit!(w_addr.street1),
                lit!(w_addr.street2),
                lit!(w_addr.city),
                lit!(w_addr.state),
                lit!(w_addr.zip),
                f2a4!(w_tax),
                f2a2!(w_ytd),
            ];
            output.push(elements);
        }
        Ok(output)
    }

    fn generate_districts(&mut self) -> Result<Records> {
        let header = vec![
            "d_id",
            "d_w_id",
            "d_name",
            "d_street_1",
            "d_street_2",
            "d_city",
            "d_state",
            "d_zip",
            "d_tax",
            "d_ytd",
            "d_next_o_id",
        ];
        let mut output = Records::new("district", header);
        for d_w_id in 1..=self.num_warehouse {
            for d_id in 1..=self.districts_per_warehouse {
                let d_name = self.random_alpha_string(6, 10);
                let d_addr = self.make_address();
                let d_tax = self.random_integer(10, 10) as f32 / 100.0;
                let d_ytd = 3_000.00_f32;
                let d_next_o_id = 3001;
                let elements = vec![
                    i2a!(d_id),
                    i2a!(d_w_id),
                    lit!(d_name),
                    lit!(d_addr.street1),
                    lit!(d_addr.street2),
                    lit!(d_addr.city),
                    lit!(d_addr.state),
                    lit!(d_addr.zip),
                    f2a4!(d_tax),
                    f2a2!(d_ytd),
                    i2a!(d_next_o_id),
                ];
                output.push(elements);
            }
        }
        Ok(output)
    }

    fn generate_customer_and_history(&mut self) -> Result<(Records, Records)> {
        let customer_header = vec![
            "c_id",
            "c_d_id",
            "c_w_id",
            "c_first",
            "c_middle",
            "c_last",
            "c_street_1",
            "c_street_2",
            "c_city",
            "c_state",
            "c_zip",
            "c_phone",
            "c_since",
            "c_credit",
            "c_credit_lim",
            "c_discount",
            "c_balance",
            "c_ytd_payment",
            "c_payment_cnt",
            "c_delivery_cnt",
            "c_data",
        ];
        let history_header = vec![
            "h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data",
        ];
        let mut customer_output = Records::new("customer", customer_header);
        let mut history_output = Records::new("history", history_header);
        for c_w_id in 1..=self.num_warehouse {
            for c_d_id in 1..self.districts_per_warehouse {
                for c_id in 1..self.customers_per_district {
                    let c_first = self.random_alpha_string(8, 16);
                    let c_middle = "OE";
                    let c_last = if c_id <= 1000 {
                        self.make_lastname(c_id - 1)
                    } else {
                        let num = self.non_uniform_integer(255, 0, 999);
                        self.make_lastname(num)
                    };
                    let c_addr = self.make_address();
                    let c_phone = self.random_number_string(13, 13);
                    let c_since = self.make_now();
                    let c_credit = if self.random_integer(0, 1) == 0 { "GC" } else { "BC" };
                    let c_credit_lim = 5000;
                    let c_discount = self.random_integer(0, 50) as f32 / 100.0;
                    let c_balance = -10.0f32;
                    let c_ytd_payment = 10.0f32;
                    let c_payment_cnt = 1;
                    let c_delivery = 0;
                    let c_data = self.random_alpha_string(300, 500);

                    let elements = vec![
                        i2a!(c_id),
                        i2a!(c_d_id),
                        i2a!(c_w_id),
                        lit!(c_first),
                        lit!(c_middle),
                        lit!(c_last),
                        lit!(c_addr.street1),
                        lit!(c_addr.street2),
                        lit!(c_addr.city),
                        lit!(c_addr.state),
                        lit!(c_addr.zip),
                        lit!(c_phone),
                        lit!(c_since.clone()),
                        lit!(c_credit),
                        i2a!(c_credit_lim),
                        f2a2!(c_discount),
                        f2a2!(c_balance),
                        f2a2!(c_ytd_payment),
                        i2a!(c_payment_cnt),
                        i2a!(c_delivery),
                        lit!(c_data),
                    ];
                    customer_output.push(elements);

                    let h_amount = 10.0f32;
                    let h_data = self.random_alpha_string(12, 24);
                    let elements = vec![
                        i2a!(c_id),
                        // customer's district id
                        i2a!(c_d_id),
                        // customer's warehouse id
                        i2a!(c_w_id),
                        // district id the payment made
                        i2a!(c_d_id),
                        // warehouse id the payment made
                        i2a!(c_w_id),
                        lit!(c_since.clone()),
                        f2a2!(h_amount),
                        lit!(h_data),
                    ];
                    history_output.push(elements);
                }
            }
        }
        Ok((customer_output, history_output))
    }

    fn generate_items(&mut self) -> Result<Records> {
        let header = vec!["i_id", "i_im_id", "i_name", "i_price", "i_data"];
        let mut output = Records::new("item", header);

        // 10% rows marked as original.
        let mut origin = vec![false; self.num_items as usize];
        for _ in 0..(self.num_items / 10) {
            loop {
                let pos = self.random_integer(0, self.num_items - 1) as usize;
                if !origin[pos] {
                    // find a slot that is not used.
                    origin[pos] = true;
                    break;
                }
            }
        }

        for i_id in 1..=self.num_items {
            let i_im_id = self.random_integer(0, 10000);
            let i_name = self.random_alpha_string(14, 24);
            let i_price = self.random_integer(100, 1000) as f32 / 100.0;
            let mut i_data = self.random_alpha_string(25, 50);
            if origin[(i_id - 1) as usize] {
                let sz = i_data.len() as i32;
                let pos = self.random_integer(0, sz - 8) as usize;
                i_data.replace_range(pos..(pos + 8), "original")
            }

            #[rustfmt::skip]
            let elements = vec![
                i2a!(i_id),
                i2a!(i_im_id),
                lit!(i_name),
                f2a2!(i_price),
                lit!(i_data),
            ];
            output.push(elements);
        }
        Ok(output)
    }

    fn generate_stock(&mut self) -> Result<Records> {
        let header = vec![
            "s_i_id",
            "s_w_id",
            "s_quantity",
            "s_dist_01",
            "s_dist_02",
            "s_dist_03",
            "s_dist_04",
            "s_dist_05",
            "s_dist_06",
            "s_dist_07",
            "s_dist_08",
            "s_dist_09",
            "s_dist_10",
            "s_ytd",
            "s_order_cnt",
            "s_remote_cnt",
            "s_data",
        ];
        let mut output = Records::new("stock", header);

        for s_w_id in 1..=self.num_warehouse {
            // 10% rows marked as original.
            let mut origin = vec![false; self.num_items as usize];
            for _ in 0..(self.num_items / 10) {
                loop {
                    let pos = self.random_integer(0, self.num_items - 1) as usize;
                    if !origin[pos] {
                        // find a slot that is not used.
                        origin[pos] = true;
                        break;
                    }
                }
            }
            for s_i_id in 1..=self.num_items {
                let s_quantity = self.random_integer(10, 100);
                let s_dist_01 = self.random_alpha_string(24, 24);
                let s_dist_02 = self.random_alpha_string(24, 24);
                let s_dist_03 = self.random_alpha_string(24, 24);
                let s_dist_04 = self.random_alpha_string(24, 24);
                let s_dist_05 = self.random_alpha_string(24, 24);
                let s_dist_06 = self.random_alpha_string(24, 24);
                let s_dist_07 = self.random_alpha_string(24, 24);
                let s_dist_08 = self.random_alpha_string(24, 24);
                let s_dist_09 = self.random_alpha_string(24, 24);
                let s_dist_10 = self.random_alpha_string(24, 24);
                let s_ytd = 0;
                let s_order_cnt = 0;
                let s_remote_cnt = 0;
                let mut s_data = self.random_alpha_string(26, 50);
                if origin[(s_i_id - 1) as usize] {
                    let sz = s_data.len() as i32;
                    let pos = self.random_integer(0, sz - 8) as usize;
                    s_data.replace_range(pos..(pos + 8), "original")
                }
                let elements = vec![
                    i2a!(s_i_id),
                    i2a!(s_w_id),
                    i2a!(s_quantity),
                    lit!(s_dist_01),
                    lit!(s_dist_02),
                    lit!(s_dist_03),
                    lit!(s_dist_04),
                    lit!(s_dist_05),
                    lit!(s_dist_06),
                    lit!(s_dist_07),
                    lit!(s_dist_08),
                    lit!(s_dist_09),
                    lit!(s_dist_10),
                    i2a!(s_ytd),
                    i2a!(s_order_cnt),
                    i2a!(s_remote_cnt),
                    lit!(s_data),
                ];
                output.push(elements);
            }
        }
        Ok(output)
    }

    fn generate_orders(&mut self) -> Result<(Records, Records, Records)> {
        let order_header = vec![
            "o_id",
            "o_d_id",
            "o_w_id",
            "o_c_id",
            "o_entry_d",
            "o_carrier_id",
            "o_ol_cnt",
            "o_all_local",
        ];
        let order_line_header = vec![
            "ol_o_id",
            "ol_d_id",
            "ol_w_id",
            "ol_number",
            "ol_i_id",
            "ol_supply_w_id",
            "ol_delivery_d",
            "ol_quantity",
            "ol_amount",
            "ol_dist_info",
        ];
        let new_order_header = vec!["no_o_id", "no_d_id", "no_w_id"];

        let mut order = Records::new("\"order\"", order_header);
        let mut order_line = Records::new("order_line", order_line_header);
        let mut new_order = Records::new("new_order", new_order_header);

        for o_w_id in 1..=self.num_warehouse {
            for o_d_id in 1..=self.districts_per_warehouse {
                // Each customer has exactly one order per district
                assert_eq!(self.customers_per_district, self.orders_per_district);
                let customer_id_permutation =
                    self.make_permutation(1, self.customers_per_district + 1);

                // last 30% as new order
                let split = self.orders_per_district - self.orders_per_district / 100 * 30;

                for o_id in 1..=self.orders_per_district {
                    let o_c_id = customer_id_permutation[(o_id - 1) as usize];
                    let o_entry_d = self.make_now();
                    let o_carrier_id =
                        if o_id > split { None } else { Some(self.random_integer(1, 10)) };
                    let o_ol_cnt = self.random_integer(5, 15);
                    let o_all_local = true;
                    let elements = vec![
                        i2a!(o_id),
                        i2a!(o_d_id),
                        i2a!(o_w_id),
                        i2a!(o_c_id),
                        lit!(o_entry_d.clone()),
                        io2a!(o_carrier_id),
                        i2a!(o_ol_cnt),
                        i2a!(o_all_local),
                    ];
                    order.push(elements);

                    for ol_number in 1..=o_ol_cnt {
                        let ol_i_id = self.random_integer(1, self.num_items);
                        let ol_quantity = 5;
                        let ol_dist_info = self.random_alpha_string(24, 24);

                        let (ol_amount, ol_delivery_d) = if o_id > split {
                            let ol_amount = self.random_integer(10, 10000) as f32 / 100.0;
                            let ol_delivery_d = None;
                            (ol_amount, ol_delivery_d)
                        } else {
                            let ol_amount = 0.0;
                            let ol_delivery_d = Some(o_entry_d.clone());
                            (ol_amount, ol_delivery_d)
                        };

                        let elements = vec![
                            i2a!(o_id),
                            i2a!(o_d_id),
                            i2a!(o_w_id),
                            i2a!(ol_number),
                            i2a!(ol_i_id),
                            i2a!(o_w_id), // supply warehouse id
                            olit!(ol_delivery_d),
                            i2a!(ol_quantity),
                            f2a2!(ol_amount),
                            lit!(ol_dist_info),
                        ];
                        order_line.push(elements);
                    }
                    if o_id > split {
                        #[rustfmt::skip]
                        let elements = vec![
                            i2a!(o_id),
                            i2a!(o_d_id),
                            i2a!(o_w_id),
                        ];
                        new_order.push(elements);
                    }
                }
            }
        }
        Ok((order, order_line, new_order))
    }

    fn random_alpha_string(&mut self, min: i32, max: i32) -> String {
        let len = self.rng.random_range(min..=max);
        (0..len).map(|_| (self.rng.random_range(b'a'..=b'z')) as char).collect()
    }

    fn random_number_string(&mut self, min: i32, max: i32) -> String {
        let len = self.rng.random_range(min..=max);
        (0..len).map(|_| (self.rng.random_range(b'0'..=b'9')) as char).collect()
    }

    fn make_address(&mut self) -> Address {
        Address {
            street1: self.random_alpha_string(10, 20),
            street2: self.random_number_string(10, 20),
            city: self.random_alpha_string(10, 20),
            state: self.random_alpha_string(2, 2),
            zip: self.random_number_string(9, 9),
        }
    }

    fn random_integer(&mut self, min: i32, max: i32) -> i32 {
        self.rng.random_range(min..=max)
    }

    // generate non-uniform random numbers, inspired by the TPC-C benchmark.
    // It combines two random numbers with a bitwise OR, adds a constant (42),
    // and maps the result into the target range. This process creates "hot spots",
    // i.e., some values are more likely than others—simulating real-world data
    // access patterns where certain records are accessed more frequently. The bitwise
    // OR and addition introduce bias, making the distribution non-uniform.
    fn non_uniform_integer(&mut self, a: i32, x: i32, y: i32) -> i32 {
        let n1 = self.random_integer(0, a);
        let n2 = self.random_integer(x, y);
        (n1 | n2) % (y - x + 1) + x
    }

    fn make_permutation(&mut self, min: i32, max: i32) -> Vec<i32> {
        assert!(max > min);
        let len = max - min;
        let mut result: Vec<i32> = (min..max).collect();
        for i in 0..result.len() {
            let j = self.rng.random_range(0..len as usize);
            result.swap(i, j);
        }
        result
    }

    fn make_lastname(&self, num: i32) -> String {
        let n = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"];
        let first = n[(num / 100) as usize];
        let second = n[((num / 10) % 10) as usize];
        let third = n[(num % 10) as usize];
        format!("{}{}{}", first, second, third)
    }

    fn make_now(&mut self) -> String {
        let dt = Utc::now();
        let out = format!("{}", dt.format("%Y-%m-%d %H:%M:%S"));
        out
    }
}

impl Default for TpccGenerator {
    fn default() -> Self {
        Self {
            num_warehouse: 10,
            num_items: 10000,
            districts_per_warehouse: 10,
            // assuming 1:1 relationship between
            // customers and orders.
            customers_per_district: 100,
            orders_per_district: 100,
            rng: SmallRng::from_os_rng(),
            sz: 5000,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::Path;

    use peak_alloc::PeakAlloc;

    use super::*;
    use crate::sql::parser::Parser;

    #[global_allocator]
    static PEAK_ALLOC: PeakAlloc = PeakAlloc;

    #[test]
    fn test_output_csv() -> Result<()> {
        let dir = "src/tpcc/testdata/csv";

        let mut gen = TpccGenerator::new(10);
        let data = gen.generate()?;

        // write csv file
        data.to_csv(dir)?;
        Ok(())
    }

    #[test]
    fn test_output_queries() -> Result<()> {
        let dir = "src/tpcc/testdata/sql";
        fs::create_dir_all(dir)?;

        let mut gen = TpccGenerator::new(10);
        let data = gen.generate()?;

        let queries = data.to_queries()?;
        // write queries to file
        for (i, q) in queries.into_iter().enumerate() {
            fs::write(Path::new(dir).join(format!("s{:03}.sql", i)), q)?;
        }
        Ok(())
    }

    #[test]
    fn test_parse_create_tables() -> Result<()> {
        let dir = "src/tpcc/testdata/create_table";
        fs::create_dir_all(dir)?;

        let mut parser = Parser::new(TPCC_TABLES)?;
        let stmts = parser.parse_statements()?;
        for (i, stmt) in stmts.iter().enumerate() {
            let mut f = fs::File::create(Path::new(dir).join(format!("table{}.sql", i + 1)))?;
            write!(f, "{:#}\n\n", stmt)?;
        }
        Ok(())
    }

    #[test]
    fn test_parse_generated_queries() -> Result<()> {
        let peak_mem_before = PEAK_ALLOC.peak_usage_as_mb();

        let mut gen = TpccGenerator::new(10);
        let data = gen.generate()?;
        let queries = data.to_queries()?;

        let peak_mem_after_gen = PEAK_ALLOC.peak_usage_as_mb();

        for (i, it) in queries.into_iter().enumerate() {
            let mut parser = Parser::new(&it)?;
            let stmts = parser.parse_statements()?;
            println!("parse query {}, #stmt {}", i, stmts.len())
        }
        let peak_mem_after_parse = PEAK_ALLOC.peak_usage_as_mb();

        println!("Memory usage:");
        println!("  Before generation: {:.2} MB", peak_mem_before);
        println!("  After generation: {:.2} MB", peak_mem_after_gen);
        println!("  After parse: {:.2} MB", peak_mem_after_parse);
        println!("  Generation used: {:.2} MB", peak_mem_after_gen - peak_mem_before);

        Ok(())
    }
}
