use std::sync::{Arc, Mutex};

use native_api_1c::{
    native_api_1c_core::ffi::connection::Connection,
    native_api_1c_macro::AddIn,
};

use tantivy::{
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{
        Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, STRING, STORED, TEXT,
        Value,          // <-- добавить
    },
    tokenizer::{LowerCaser, NgramTokenizer, TextAnalyzer},
    Index, IndexWriter, TantivyDocument,
};

// ============================================
// ДВИЖОК ПОИСКА (два индекса: обычный и N-граммный)
// ============================================
struct SearchEngine {
    // --- старый (обычный) индекс ---
    index: Option<Index>,
    text_field: Option<Field>,
    text_raw_field: Option<Field>,
    id_field: Option<Field>,
    total: usize,

    // --- новый (N-граммный) индекс ---
    ngram_index: Option<Index>,
    ngram_text_field: Option<Field>,
    ngram_text_raw_field: Option<Field>,
    ngram_id_field: Option<Field>,
    ngram_total: usize,

    // Параметры N-грамм
    ngram_min: usize,
    ngram_max: usize,
    ngram_tokenizer_name: String,
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self {
            index: None,
            text_field: None,
            text_raw_field: None,
            id_field: None,
            total: 0,

            ngram_index: None,
            ngram_text_field: None,
            ngram_text_raw_field: None,
            ngram_id_field: None,
            ngram_total: 0,

            ngram_min: 3,
            ngram_max: 3,
            ngram_tokenizer_name: "ngram3".to_string(),
        }
    }
}

impl SearchEngine {
    // --------------------------------------------
    // Регистрация N-граммного токенизатора
    // --------------------------------------------
    fn register_ngram_tokenizer(index: &Index, name: &str, min: usize, max: usize) {
        let ngram = NgramTokenizer::new(min, max, false)
            .expect("Не удалось создать NgramTokenizer");
        let analyzer = TextAnalyzer::builder(ngram)
            .filter(LowerCaser)
            .build();
        index.tokenizers().register(name, analyzer);
    }

    // --------------------------------------------
    // TextOptions для N-граммного поля
    // --------------------------------------------
    fn ngram_text_options(tokenizer_name: &str) -> TextOptions {
        let indexing = TextFieldIndexing::default()
            .set_tokenizer(tokenizer_name)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        TextOptions::default()
            .set_indexing_options(indexing)
            .set_stored()
    }

    // --------------------------------------------
    // Поиск по СТАРОМУ (обычному) индексу
    // --------------------------------------------
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

            let top = searcher.search(
                &q,
                &TopDocs::with_limit(limit.max(1)).order_by_score(),
            )?;

            let mut hits = Vec::new();
            for (score, addr) in top {
                // Явная аннотация типа нужна из-за изменения API в 0.26
                let document: TantivyDocument = searcher.doc(addr)?;

                let id_value = document
                    .get_first(id_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let text_value = document
                    .get_first(text_raw_field)
                    .and_then(|v| v.as_str())
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

    // --------------------------------------------
    // Поиск по N-ГРАММНОМУ индексу
    // --------------------------------------------
    fn search_ngram(&self, query: &str, limit: usize) -> Result<String, String> {
        let result = (|| -> Result<String, Box<dyn std::error::Error>> {
            let index = self
                .ngram_index
                .as_ref()
                .ok_or("N-граммный индекс не построен")?;
            let text_field = self
                .ngram_text_field
                .ok_or("N-граммный индекс не построен")?;
            let text_raw_field = self
                .ngram_text_raw_field
                .ok_or("N-граммный индекс не построен")?;
            let id_field = self
                .ngram_id_field
                .ok_or("N-граммный индекс не построен")?;

            let reader = index.reader()?;
            let searcher = reader.searcher();

            let parser = QueryParser::for_index(index, vec![text_field]);
            let q = parser.parse_query(query)?;

            let top = searcher.search(
                &q,
                &TopDocs::with_limit(limit.max(1)).order_by_score(),
            )?;

            let mut hits = Vec::new();
            for (score, addr) in top {
                // Явная аннотация типа
                let document: TantivyDocument = searcher.doc(addr)?;

                let id_value = document
                    .get_first(id_field)
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let text_value = document
                    .get_first(text_raw_field)
                    .and_then(|v| v.as_str())
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
            Err(e) => Err(format!("Ошибка N-граммного поиска: {}", e)),
        }
    }

    // --------------------------------------------
    // Построение ОБОИХ индексов
    // --------------------------------------------
    fn build_index_from_docs(&mut self, docs: &[serde_json::Value]) -> Result<usize, String> {
        self.build_regular_index(docs)?;
        self.build_ngram_index(docs)?;
        Ok(docs.len())
    }

    fn build_regular_index(
        &mut self,
        docs: &[serde_json::Value],
    ) -> Result<usize, String> {
        let result = (|| -> Result<usize, Box<dyn std::error::Error>> {
            let mut schema_builder = Schema::builder();
            let id_field = schema_builder.add_text_field("id", STRING | STORED);
            let text_field = schema_builder.add_text_field("text", TEXT | STORED);
            let text_raw_field =
                schema_builder.add_text_field("text_raw", STRING | STORED);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);
            let mut writer: IndexWriter = index.writer(50_000_000)?;

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
            writer.wait_merging_threads()?;

            self.index = Some(index);
            self.text_field = Some(text_field);
            self.text_raw_field = Some(text_raw_field);
            self.id_field = Some(id_field);
            self.total = docs.len();

            Ok(docs.len())
        })();

        result.map_err(|e| format!("Ошибка старого индекса: {}", e))
    }

    fn build_ngram_index(
        &mut self,
        docs: &[serde_json::Value],
    ) -> Result<usize, String> {
        let result = (|| -> Result<usize, Box<dyn std::error::Error>> {
            let mut schema_builder = Schema::builder();
            let id_field = schema_builder.add_text_field("id", STRING | STORED);

            let text_options = Self::ngram_text_options(&self.ngram_tokenizer_name);
            let text_field = schema_builder.add_text_field("text", text_options);

            let text_raw_field =
                schema_builder.add_text_field("text_raw", STRING | STORED);
            let schema = schema_builder.build();

            let index = Index::create_in_ram(schema);

            Self::register_ngram_tokenizer(
                &index,
                &self.ngram_tokenizer_name,
                self.ngram_min,
                self.ngram_max,
            );

            let mut writer: IndexWriter = index.writer(50_000_000)?;

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
            writer.wait_merging_threads()?;

            self.ngram_index = Some(index);
            self.ngram_text_field = Some(text_field);
            self.ngram_text_raw_field = Some(text_raw_field);
            self.ngram_id_field = Some(id_field);
            self.ngram_total = docs.len();

            Ok(docs.len())
        })();

        result.map_err(|e| format!("Ошибка N-граммного индекса: {}", e))
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

    #[add_in_func(name = "SearchNgram", name_ru = "ПоискНГрамм")]
    #[arg(Str)]
    #[arg(Int, default = 20)]
    #[returns(Str, result)]
    pub search_ngram: fn(&Self, String, i32) -> Result<String, ()>,

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

    #[add_in_func(name = "CountNgram", name_ru = "КоличествоНГрамм")]
    #[returns(Int, result)]
    pub count_ngram: fn(&Self) -> Result<i32, ()>,
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
            search_ngram: Self::search_ngram_inner,
            add: Self::add_inner,
            remove: Self::remove_inner,
            count: Self::count_inner,
            count_ngram: Self::count_ngram_inner,
        }
    }
}

// ============================================
// Безопасная обёртка над паникой на границе FFI с 1С
// ============================================
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

    fn search_ngram_inner(&self, query: String, limit: i32) -> Result<String, ()> {
        Ok(catch("[]".to_string(), || {
            if query.trim().is_empty() {
                return "[]".to_string();
            }
            if limit <= 0 {
                return "[]".to_string();
            }
            let safe_limit = (limit as usize).min(10_000);

            let engine = match self.engine.lock() {
                Ok(e) => e,
                Err(_) => return "[]".to_string(),
            };

            match engine.search_ngram(&query, safe_limit) {
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

        // --- Старый индекс ---
        if let (Some(index), Some(id_field), Some(text_field), Some(text_raw_field)) = (
            engine.index.as_ref(),
            engine.id_field,
            engine.text_field,
            engine.text_raw_field,
        ) {
            if let Ok(mut writer) = index.writer::<TantivyDocument>(50_000_000) {
                let ok = writer
                    .add_document(doc!(
                        id_field => id.clone(),
                        text_field => text.clone(),
                        text_raw_field => text.clone()
                    ))
                    .is_ok()
                    && writer.commit().is_ok()
                    && writer.wait_merging_threads().is_ok();
                if !ok {
                    return 0;
                }
            }
        }

        // --- N-граммный индекс ---
        if let (Some(index), Some(id_field), Some(text_field), Some(text_raw_field)) = (
            engine.ngram_index.as_ref(),
            engine.ngram_id_field,
            engine.ngram_text_field,
            engine.ngram_text_raw_field,
        ) {
            if let Ok(mut writer) = index.writer::<TantivyDocument>(50_000_000) {
                let ok = writer
                    .add_document(doc!(
                        id_field => id.clone(),
                        text_field => text.clone(),
                        text_raw_field => text.clone()
                    ))
                    .is_ok()
                    && writer.commit().is_ok()
                    && writer.wait_merging_threads().is_ok();
                if !ok {
                    return 0;
                }
            }
        }

        engine.total += 1;
        engine.ngram_total += 1;
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

        // --- Старый индекс ---
        if let (Some(index), Some(id_field)) = (engine.index.as_ref(), engine.id_field) {
            if let Ok(mut writer) = index.writer::<TantivyDocument>(50_000_000) {
                let term = tantivy::Term::from_field_text(id_field, &id);
                writer.delete_term(term);
                if writer.commit().is_err() || writer.wait_merging_threads().is_err() {
                    return 0;
                }
            }
        }

        // --- N-граммный индекс ---
        if let (Some(index), Some(id_field)) =
            (engine.ngram_index.as_ref(), engine.ngram_id_field)
        {
            if let Ok(mut writer) = index.writer::<TantivyDocument>(50_000_000) {
                let term = tantivy::Term::from_field_text(id_field, &id);
                writer.delete_term(term);
                if writer.commit().is_err() || writer.wait_merging_threads().is_err() {
                    return 0;
                }
            }
        }

        engine.total = engine.total.saturating_sub(1);
        engine.ngram_total = engine.ngram_total.saturating_sub(1);
        1
    }

    fn count_inner(&self) -> Result<i32, ()> {
        match self.engine.lock() {
            Ok(engine) => Ok(engine.total as i32),
            Err(_) => Ok(0),
        }
    }

    fn count_ngram_inner(&self) -> Result<i32, ()> {
        match self.engine.lock() {
            Ok(engine) => Ok(engine.ngram_total as i32),
            Err(_) => Ok(0),
        }
    }
}