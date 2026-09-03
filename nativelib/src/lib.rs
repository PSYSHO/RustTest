use std::sync::{Arc, Mutex};

// Импорты для макросов библиотеки Sebekerga
use native_api_1c::{
    native_api_1c_core::ffi::connection::Connection,
    native_api_1c_macro::{extern_functions, AddIn},
};

use tantivy::{
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{Field, Schema, STORED, TEXT},
    Index,
};

// ============================================
// ДВИЖОК ПОИСКА
// ============================================
struct SearchEngine {
    index: Option<Index>,
    text_field: Option<Field>,
    id_field: Option<Field>,
    extra_field: Option<Field>,
    total: usize,
}

impl Default for SearchEngine {
    fn default() -> Self {
        Self {
            index: None,
            text_field: None,
            id_field: None,
            extra_field: None,
            total: 0,
        }
    }
}

impl SearchEngine {
    fn search(&self, query: &str, limit: usize) -> Result<String, Box<dyn std::error::Error>> {
        let index = self
            .index
            .as_ref()
            .ok_or("Индекс не построен")?;
        let text_field = self.text_field.unwrap();
        let id_field = self.id_field.unwrap();
        let extra_field = self.extra_field.unwrap();

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let parser = QueryParser::for_index(index, vec![text_field]);
        let q = parser.parse_query(query)?;

        let top = searcher.search(&q, &TopDocs::with_limit(limit.max(1)))?;

        let mut hits = Vec::new();
        for (score, addr) in top {
            let document = searcher.doc(addr)?;
            
            hits.push(serde_json::json!({
                "id": document.get_first(id_field).map(|v| v.as_text().unwrap_or("")).unwrap_or(""),
                "text": document.get_first(text_field).map(|v| v.as_text().unwrap_or("")).unwrap_or(""),
                "extra": document.get_first(extra_field).map(|v| v.as_text().unwrap_or("")).unwrap_or(""),
                "score": score,
            }));
        }

        Ok(serde_json::to_string(&hits)?)
    }

    fn build_index_from_docs(&mut self, docs: &[serde_json::Value]) -> Result<usize, Box<dyn std::error::Error>> {
        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", STORED);
        let text_field = schema_builder.add_text_field("text", TEXT);
        let extra_field = schema_builder.add_text_field("extra", STORED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let mut writer = index.writer(50_000_000)?;

        for d in docs {
            let id = d.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let text = d.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let extra = d.get("extra").and_then(|v| v.as_str()).unwrap_or("");
            writer.add_document(doc!(
                id_field => id.to_string(),
                text_field => text.to_string(),
                extra_field => extra.to_string()
            ))?;
        }

        writer.commit()?;
        drop(writer);

        self.index = Some(index);
        self.text_field = Some(text_field);
        self.id_field = Some(id_field);
        self.extra_field = Some(extra_field);
        self.total = docs.len();

        Ok(docs.len())
    }
}

// ============================================
// ОСНОВНАЯ КОМПОНЕНТА 1С
// ============================================
#[derive(AddIn)]
pub struct NativeApiSearch {
    #[add_in_con]
    connection: Arc<Option<&'static Connection>>,

    // Свойство для передачи JSON коллекции
    #[add_in_prop(ty = Str, name = "CollectionJson", name_ru = "КоллекцияJSON", readable, writable)]
    pub collection_json: String,

    // Движок поиска
    engine: Arc<Mutex<SearchEngine>>,

    // Функции компоненты
    #[add_in_func(name = "BuildIndex", name_ru = "ПостроитьИндекс")]
    #[returns(ty = Int, result)]
    pub build_index: fn(&mut Self) -> Result<i32, ()>,

    #[add_in_func(name = "Search", name_ru = "Поиск")]
    #[arg(ty = Str)]
    #[arg(ty = Int)]
    #[returns(ty = Str, result)]
    pub search: fn(&Self, String, i32) -> Result<String, ()>,

    #[add_in_func(name = "Add", name_ru = "Добавить")]
    #[arg(ty = Str)]
    #[arg(ty = Str)]
    #[arg(ty = Str)]
    #[returns(ty = Int, result)]
    pub add: fn(&mut Self, String, String, String) -> Result<i32, ()>,

    #[add_in_func(name = "Remove", name_ru = "Удалить")]
    #[arg(ty = Str)]
    #[returns(ty = Int, result)]
    pub remove: fn(&mut Self, String) -> Result<i32, ()>,

    #[add_in_func(name = "Count", name_ru = "Количество")]
    #[returns(ty = Int, result)]
    pub count: fn(&Self) -> Result<i32, ()>,
}

impl Default for NativeApiSearch {
    fn default() -> Self {
        Self {
            connection: Arc::new(None),
            collection_json: String::new(),
            engine: Arc::new(Mutex::new(SearchEngine::default())),
            build_index: Self::build_index_inner,
            search: Self::search_inner,
            add: Self::add_inner,
            remove: Self::remove_inner,
            count: Self::count_inner,
        }
    }
}

// Реализация методов
impl NativeApiSearch {
    fn build_index_inner(&mut self) -> Result<i32, ()> {
        let docs: Vec<serde_json::Value> = serde_json::from_str(&self.collection_json)
            .map_err(|_| ())?;

        let mut engine = self.engine.lock().unwrap();
        let count = engine.build_index_from_docs(&docs).map_err(|_| ())?;
        Ok(count as i32)
    }

    fn search_inner(&self, query: String, limit: i32) -> Result<String, ()> {
        let engine = self.engine.lock().unwrap();
        engine.search(&query, limit as usize).map_err(|_| ())
    }

    fn add_inner(&mut self, id: String, text: String, extra: String) -> Result<i32, ()> {
        let mut engine = self.engine.lock().unwrap();

        let index = engine
            .index
            .as_ref()
            .ok_or(())?;
        let id_field = engine.id_field.unwrap();
        let text_field = engine.text_field.unwrap();
        let extra_field = engine.extra_field.unwrap();

        let mut writer = index.writer(50_000_000).map_err(|_| ())?;
        writer.add_document(doc!(
            id_field => id,
            text_field => text,
            extra_field => extra
        )).map_err(|_| ())?;
        writer.commit().map_err(|_| ())?;
        drop(writer);

        engine.total += 1;
        Ok(1)
    }

    fn remove_inner(&mut self, id: String) -> Result<i32, ()> {
        let mut engine = self.engine.lock().unwrap();

        let index = engine
            .index
            .as_ref()
            .ok_or(())?;
        let id_field = engine.id_field.unwrap();

        let mut writer = index.writer(50_000_000).map_err(|_| ())?;
        let term = tantivy::Term::from_field_text(id_field, &id);
        writer.delete_term(term);
        writer.commit().map_err(|_| ())?;
        drop(writer);

        engine.total = engine.total.saturating_sub(1);
        Ok(1)
    }

    fn count_inner(&self) -> Result<i32, ()> {
        Ok(self.engine.lock().unwrap().total as i32)
    }
}

// ============================================
// ЭКСПОРТ ФУНКЦИЙ ДЛЯ 1С
// ============================================
extern_functions! {
    NativeApiSearch::default(),
}