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

use adbc_core::error::{Error, Status};

pub fn adbc_err(status: Status, msg: impl Into<String>) -> Error {
    Error::with_message_and_status(msg, status)
}

pub fn not_implemented(feature: &str) -> Error {
    adbc_err(
        Status::NotImplemented,
        format!("{feature} is not supported"),
    )
}

pub fn invalid_state(msg: impl Into<String>) -> Error {
    adbc_err(Status::InvalidState, msg)
}

pub fn io_err(msg: impl Into<String>) -> Error {
    adbc_err(Status::IO, msg)
}
