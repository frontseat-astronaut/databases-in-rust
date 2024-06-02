use super::KVDb;
use rand::Rng;

pub struct Test {
    db: Box<dyn KVDb>,
}

// TODO: deprecate this soon
impl Test {
    pub fn new(db: Box<dyn KVDb>) -> Test {
        Test { db }
    }

    pub fn run(&mut self) {
        println!("starting test");
        self.get_value_for_test("k1");
        self.get_value_for_test("k2");
        self.get_value_for_test("k3");
        self.get_value_for_test("k4");

        /* these cases throw errors */
        // self.set_key_value_for_test("k,", "v");
        // self.get_value_for_test("k,");
        // self.set_key_value_for_test("k", "🪦");
        // self.get_value_for_test("k");

        self.set_key_value_for_test("k1", "v11");
        self.get_value_for_test("k1");
        self.set_key_value_for_test("k1", "v12");
        self.get_value_for_test("k1");

        self.set_random_value_for_test("k4");
        self.get_value_for_test("k4");

        self.set_random_value_for_test("k3");
        self.get_value_for_test("k3");

        self.set_key_value_for_test("k2", "v21");
        self.get_value_for_test("k2");
        self.delete_key_value_for_test("k2");
        self.get_value_for_test("k2");

        println!("");
    }

    fn set_random_value_for_test(&mut self, key: &str) {
        self.set_key_value_for_test(key, &Self::create_random_value())
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

    fn create_random_value() -> String {
        format!("{}", rand::thread_rng().gen_range(1..100000))
    }
}
