// Copyright (c) 2026 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use adbc_core::error::Result;
use adbc_core::options::{OptionDatabase, OptionValue};
use adbc_core::Driver;
use tokio::runtime::Runtime;

use crate::database::MongoDBDatabase;

#[derive(Default)]
pub struct MongoDBDriver {
    runtime: Option<Arc<Runtime>>,
}

impl MongoDBDriver {
    fn ensure_runtime(&mut self) -> Result<Arc<Runtime>> {
        if let Some(rt) = &self.runtime {
            return Ok(Arc::clone(rt));
        }
        let rt = Runtime::new()
            .map_err(|e| crate::error::io_err(format!("failed to create tokio runtime: {e}")))?;
        let rt = Arc::new(rt);
        self.runtime = Some(Arc::clone(&rt));
        Ok(rt)
    }
}

impl Driver for MongoDBDriver {
    type DatabaseType = MongoDBDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        let runtime = self.ensure_runtime()?;
        Ok(MongoDBDatabase::new(runtime))
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let runtime = self.ensure_runtime()?;
        let mut db = MongoDBDatabase::new(runtime);
        for (key, value) in opts {
            db.set_option(key, value)?;
        }
        Ok(db)
    }
}

use adbc_core::Optionable;
