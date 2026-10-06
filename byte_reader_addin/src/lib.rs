use std::sync::{Arc, Mutex};

use native_api_1c::{
    native_api_1c_core::ffi::connection::Connection,
    native_api_1c_macro::AddIn,
};

use tantivy::{
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{Field, Schema, STRING, STORED, TEXT},
    Index,
};

// ============================================
// ДВИЖОК ПОИСКА
// ============================================
struct SearchEngine {
    index: Option<Index>,
    text_field: Option<Field>,      // токенизированное поле для поиска
    text_raw_field: Option<Field>,  // raw-поле для возврата оригинала
    id_field: Option<Field>,
    total: usize,
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self {
            index: None,
            text_field: None,
            text_raw_field: None,
            id_field: None,
            total: 0,
        }
    }
}

impl SearchEngine {
    fn search(&self, query: &str, limit: usize) -> Result<String, String> {
        let result = (|| -> Result<String, Box<dyn std::error::Error>> {
            let index = self.index.as_ref().ok_or("Индекс не построен")?;
            let text_field = self.text_field.ok_or("Индекс не построен")?;
            let text_raw_field = self.text_raw_field.ok_or("Индекс не построен")?;
            let id_field = self.id_field.ok_or("Индекс не построен")?;

            let reader = index.reader()?;
            let searcher = reader.searcher();

            let parser = QueryParser::for_index(index, vec![text_field]);
            let q = parser.parse_query(query)?;

            let top = searcher.search(&q, &TopDocs::with_limit(limit.max(1)))?;

            let mut hits = Vec::new();
            for (score, addr) in top {
                let document = searcher.doc(addr)?;

                let id_value = document
                    .get_first(id_field)
                    .and_then(|v| v.as_text())
                    .unwrap_or("")
                    .to_string();

                // Оригинальный текст берём из raw-поля (STRING | STORED),
                // для которого as_text() гарантированно работает
                let text_value = document
                    .get_first(text_raw_field)
                    .and_then(|v| v.as_text())
                    .unwrap_or("")
                    .to_string();

                hits.push(serde_json::json!({
                    "id": id_value,
                    "text": text_value,
                    "score": score,
                }));
            }

            Ok(serde_json::to_string(&hits)?)
        })();

        match result {
            Ok(data) => Ok(data),
            Err(e) => Err(format!("Ошибка: {}", e)),
        }
    }

    fn build_index_from_docs(&mut self, docs: &[serde_json::Value]) -> Result<usize, String> {
        let result = (|| -> Result<usize, Box<dyn std::error::Error>> {
            let mut schema_builder = Schema::builder();
            let id_field = schema_builder.add_text_field("id", STRING | STORED);
            // Токенизированное поле — по нему ищем
            let text_field = schema_builder.add_text_field("text", TEXT | STORED);
            // Raw-поле — из него возвращаем оригинал без токенизации
            let text_raw_field = schema_builder.add_text_field("text_raw", STRING | STORED);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);
            let mut writer = index.writer(50_000_000)?;

            for d in docs {
                let id = d.get("id").and_then(|v| v.as_str()).unwrap_or("");
                let text = d.get("text").and_then(|v| v.as_str()).unwrap_or("");
                writer.add_document(doc!(
                    id_field => id.to_string(),
                    text_field => text.to_string(),
                    text_raw_field => text.to_string()
                ))?;
            }

            writer.commit()?;
            // commit() возвращается сразу после записи сегментов, но tantivy
            // продолжает мёрж/компактацию в фоновых потоках. Если следующий
            // вызов (Search) прилетает почти мгновенно после BuildIndex, он
            // может пересечься с этими потоками на общей in-RAM директории —
            // гонка, которая непредсказуемо портит память (отсюда падения
            // 1CV8.exe, которые исчезают под отладчиком: пауза на точке
            // останова просто даёт фоновым потокам время завершиться).
            // wait_merging_threads() блокируется до их полного завершения.
            writer.wait_merging_threads()?;

            self.index = Some(index);
            self.text_field = Some(text_field);
            self.text_raw_field = Some(text_raw_field);
            self.id_field = Some(id_field);
            self.total = docs.len();

            Ok(docs.len())
        })();

        match result {
            Ok(count) => Ok(count),
            Err(e) => Err(format!("Ошибка: {}", e)),
        }
    }
}

// ============================================
// КОМПОНЕНТА 1С
// ============================================
#[derive(AddIn)]
pub struct NativeApiSearch {
    #[add_in_con]
    connection: Arc<Option<&'static Connection>>,

    #[add_in_prop(ty = Str, name = "CollectionJson", name_ru = "КоллекцияJSON", readable, writable)]
    pub collection_json: String,

    engine: Arc<Mutex<SearchEngine>>,

    #[add_in_func(name = "HelloWorld", name_ru = "ПриветМир")]
    #[returns(Str, result)]
    pub hello_world: fn(&Self) -> Result<String, ()>,

    #[add_in_func(name = "BuildIndex", name_ru = "ПостроитьИндекс")]
    #[returns(Int, result)]
    pub build_index: fn(&mut Self) -> Result<i32, ()>,

    #[add_in_func(name = "Search", name_ru = "Поиск")]
    #[arg(Str)]
    #[arg(Int, default = 20)]
    #[returns(Str, result)]
    pub search: fn(&Self, String, i32) -> Result<String, ()>,

    #[add_in_func(name = "Add", name_ru = "Добавить")]
    #[arg(Str)]
    #[arg(Str)]
    #[returns(Int, result)]
    pub add: fn(&mut Self, String, String) -> Result<i32, ()>,

    #[add_in_func(name = "Remove", name_ru = "Удалить")]
    #[arg(Str)]
    #[returns(Int, result)]
    pub remove: fn(&mut Self, String) -> Result<i32, ()>,

    #[add_in_func(name = "Count", name_ru = "Количество")]
    #[returns(Int, result)]
    pub count: fn(&Self) -> Result<i32, ()>,
}

impl NativeApiSearch {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for NativeApiSearch {
    fn default() -> Self {
        Self {
            connection: Arc::new(None),
            collection_json: String::new(),
            engine: Arc::new(Mutex::new(SearchEngine::default())),
            hello_world: Self::hello_world_inner,
            build_index: Self::build_index_inner,
            search: Self::search_inner,
            add: Self::add_inner,
            remove: Self::remove_inner,
            count: Self::count_inner,
        }
    }
}

// Границы FFI, которые генерирует native_api_1c для вызовов из 1С, не
// рассчитаны на прохождение через них паники Rust: `panic=unwind`
// разворачивает стек через чужой (1С-шный) SEH-фрейм, что на 32-битной
// платформе приводит к повреждению цепочки обработчиков исключений и
// падению 1CV8.exe (STATUS_INVALID_EXCEPTION_HANDLER / ACCESS_VIOLATION)
// вместо управляемой ошибки. Поэтому каждый метод, вызываемый из 1С,
// ловит панику на границе и возвращает безопасное значение по умолчанию.
fn catch<T>(default: T, f: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or(default)
}

impl NativeApiSearch {
    fn hello_world_inner(&self) -> Result<String, ()> {
        Ok("Hello World from Rust!".to_string())
    }

    fn build_index_inner(&mut self) -> Result<i32, ()> {
        Ok(catch(-4, || {
            let docs: Vec<serde_json::Value> = match serde_json::from_str(&self.collection_json) {
                Ok(d) => d,
                Err(_) => return -1,
            };

            let mut engine = match self.engine.lock() {
                Ok(e) => e,
                Err(_) => return -2,
            };

            match engine.build_index_from_docs(&docs) {
                Ok(count) => count as i32,
                Err(_) => -3,
            }
        }))
    }

    fn search_inner(&self, query: String, limit: i32) -> Result<String, ()> {
        Ok(catch("[]".to_string(), || {
            if query.trim().is_empty() {
                return "[]".to_string();
            }

            if limit <= 0 {
                return "[]".to_string();
            }
            // Верхняя граница защищает от чрезмерного выделения памяти в
            // коллекторе tantivy, если из 1С случайно передадут огромный
            // лимит; в остальном значение полностью управляется параметром.
            let safe_limit = (limit as usize).min(10_000);

            let engine = match self.engine.lock() {
                Ok(e) => e,
                Err(_) => return "[]".to_string(),
            };

            match engine.search(&query, safe_limit) {
                Ok(result) => result,
                Err(_) => "[]".to_string(),
            }
        }))
    }

    fn add_inner(&mut self, id: String, text: String) -> Result<i32, ()> {
        Ok(catch(0, || self.add_inner_impl(id, text)))
    }

    fn add_inner_impl(&mut self, id: String, text: String) -> i32 {
        let mut engine = match self.engine.lock() {
            Ok(e) => e,
            Err(_) => return 0,
        };

        let index = match engine.index.as_ref() {
            Some(i) => i,
            None => return 0,
        };
        let id_field = match engine.id_field {
            Some(f) => f,
            None => return 0,
        };
        let text_field = match engine.text_field {
            Some(f) => f,
            None => return 0,
        };
        let text_raw_field = match engine.text_raw_field {
            Some(f) => f,
            None => return 0,
        };

        let mut writer = match index.writer(50_000_000) {
            Ok(w) => w,
            Err(_) => return 0,
        };

        if let Err(_) = writer.add_document(doc!(
            id_field => id,
            text_field => text.clone(),
            text_raw_field => text
        )) {
            return 0;
        }

        if let Err(_) = writer.commit() {
            return 0;
        }
        if let Err(_) = writer.wait_merging_threads() {
            return 0;
        }

        engine.total += 1;
        1
    }

    fn remove_inner(&mut self, id: String) -> Result<i32, ()> {
        Ok(catch(0, || self.remove_inner_impl(id)))
    }

    fn remove_inner_impl(&mut self, id: String) -> i32 {
        let mut engine = match self.engine.lock() {
            Ok(e) => e,
            Err(_) => return 0,
        };

        let index = match engine.index.as_ref() {
            Some(i) => i,
            None => return 0,
        };
        let id_field = match engine.id_field {
            Some(f) => f,
            None => return 0,
        };

        let mut writer = match index.writer(50_000_000) {
            Ok(w) => w,
            Err(_) => return 0,
        };

        let term = tantivy::Term::from_field_text(id_field, &id);
        writer.delete_term(term);
        if let Err(_) = writer.commit() {
            return 0;
        }
        if let Err(_) = writer.wait_merging_threads() {
            return 0;
        }

        engine.total = engine.total.saturating_sub(1);
        1
    }

    fn count_inner(&self) -> Result<i32, ()> {
        match self.engine.lock() {
            Ok(engine) => Ok(engine.total as i32),
            Err(_) => Ok(0),
        }
    }
}