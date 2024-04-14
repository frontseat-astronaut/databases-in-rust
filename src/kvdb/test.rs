use super::KVDb;

pub struct Test<T: KVDb> {
    db: T 
}

impl<T: KVDb> Test<T> {
    pub fn new(db: T) -> Test<T> {
        Test {
            db,
        }
    }

    pub fn run(&mut self) {
        println!("starting test");
        self.set_key_value_for_test("k,", "v");
        self.get_value_for_test("k,");
        self.set_key_value_for_test("k", "🪦");
        self.get_value_for_test("k");
        self.get_value_for_test("k1");
        self.get_value_for_test("k2");
        self.set_key_value_for_test("k1", "v11");
        self.get_value_for_test("k1");
        self.set_key_value_for_test("k1", "v12");
        self.get_value_for_test("k1");
        self.set_key_value_for_test("k2", "v21");
        self.get_value_for_test("k2");
        self.delete_key_value_for_test("k2");
        self.get_value_for_test("k2");
        println!("");
    }

    fn set_key_value_for_test(&mut self, key: &str, value: &str) {
        match self.db.set(key, value) {
            Ok(()) => {
                println!("set value {} for key {} successfully", value, key)
            }
            Err(e) => {
                println!("error setting value for key {}: {}", key, e)
            }
        }
    }
    fn delete_key_value_for_test(&mut self, key: &str) {
        match self.db.delete(key) {
            Ok(()) => {
                println!("deleted key {} successfully", key)
            }
            Err(e) => {
                println!("error deleting key {}: {}", key, e)
            }
        }
    }
    fn get_value_for_test(&mut self, key: &str) {
        match self.db.get(key) {
            Ok(Some(value)) => {
                println!("key {} has value {}", key, value)
            }
            Ok(None) => {
                println!("key {} does not exist", key)
            }
            Err(e) => {
                println!("error getting value for key {}: {}", key, e)
            }
        }
    }
}