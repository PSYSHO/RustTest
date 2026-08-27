use std::sync::{Arc, Mutex};

use native_api_1c::{
    native_api_1c_core::ffi::connection::Connection,
    native_api_1c_macro::AddIn,
};
use tantivy::{
    collector::TopDocs,
    doc,
    query::QueryParser,
    schema::{Field, Schema, TantivyDocument, Value, STORED, TEXT},
    Index,
};

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
            .ok_or("Индекс не построен. Сначала вызовите ПостроитьИндекс().")?;
        let text_field = self.text_field.unwrap();
        let id_field = self.id_field.unwrap();
        let extra_field = self.extra_field.unwrap();

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
            let document = searcher.doc::<TantivyDocument>(addr)?;
            hits.push(serde_json::json!({
               "id": document.get_first(id_field).and_then(|v| v.as_str()).unwrap_or(""),
                "text": document.get_first(text_field).and_then(|v| v.as_str()).unwrap_or(""),
                "extra": document.get_first(extra_field).and_then(|v| v.as_str()).unwrap_or(""),
                "score": score,
            }));
        }

        Ok(serde_json::to_string(&hits)?)
    }
}

#[derive(AddIn)]
pub struct NativeApiSearch {
    #[add_in_con]
    connection: Arc<Option<&'static Connection>>,

    engine: Arc<Mutex<SearchEngine>>,

    #[add_in_prop(name = "CollectionJson", name_ru = "КоллекцияJSON", readable, writable)]
    collection_json: String,

    #[add_in_func(name = "BuildIndex", name_ru = "ПостроитьИндекс")]
    #[returns(Int, result)]
    build_index: fn(&Self) -> Result<i32, Box<dyn std::error::Error>>,

    #[add_in_func(name = "Search", name_ru = "Поиск")]
    #[arg(Str)]
    #[arg(Int)]
    #[returns(Str, result)]
    search: fn(&Self, String, i32) -> Result<String, Box<dyn std::error::Error>>,

    #[add_in_func(name = "Add", name_ru = "Добавить")]
    #[arg(Str)]
    #[arg(Str)]
    #[arg(Str)]
    #[returns(Int, result)]
    add: fn(&Self, String, String, String) -> Result<i32, Box<dyn std::error::Error>>,

    #[add_in_func(name = "Remove", name_ru = "Удалить")]
    #[arg(Str)]
    #[returns(Int, result)]
    remove: fn(&Self, String) -> Result<i32, Box<dyn std::error::Error>>,

    #[add_in_func(name = "Count", name_ru = "Количество")]
    #[returns(Int, result)]
    count: fn(&Self) -> Result<i32, Box<dyn std::error::Error>>,
}

impl NativeApiSearch {
    pub fn new() -> Self {
        Self {
            connection: Arc::new(None),
            engine: Arc::new(Mutex::new(SearchEngine::default())),
            collection_json: String::new(),
            build_index: |this| this.do_build_index(),
            search: |this, query, limit| this.do_search(&query, limit),
            add: |this, id: String, text: String, extra: String| this.do_add(&id, &text, &extra),
            remove: |this, id: String| this.do_remove(&id),
            count: |this| Ok(this.engine.lock().unwrap().total as i32),
        }
    }

    fn do_build_index(&self) -> Result<i32, Box<dyn std::error::Error>> {
        let docs: Vec<serde_json::Value> = serde_json::from_str(&self.collection_json)
            .map_err(|e| -> Box<dyn std::error::Error> {
                format!("Ошибка разбора КоллекцияJSON: {}", e).into()
            })?;

        let mut schema_builder = Schema::builder();
        let id_field = schema_builder.add_text_field("id", STORED);
        let text_field = schema_builder.add_text_field("text", TEXT);
        let extra_field = schema_builder.add_text_field("extra", STORED);
        let schema = schema_builder.build();

        let index = Index::create_in_ram(schema);
        let mut writer: tantivy::IndexWriter<TantivyDocument> = index.writer(50_000_000)?;

        for d in &docs {
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

        let mut engine = self.engine.lock().unwrap();
        engine.index = Some(index);
        engine.text_field = Some(text_field);
        engine.id_field = Some(id_field);
        engine.extra_field = Some(extra_field);
        engine.total = docs.len();

        Ok(docs.len() as i32)
    }

    fn do_search(&self, query: &str, limit: i32) -> Result<String, Box<dyn std::error::Error>> {
        let engine = self.engine.lock().unwrap();
        engine.search(query, limit as usize)
    }

    fn do_add(&self, id: &str, text: &str, extra: &str) -> Result<i32, Box<dyn std::error::Error>> {
        let mut engine = self.engine.lock().unwrap();

        let index = engine
            .index
            .as_ref()
            .ok_or("Индекс не построен. Сначала вызовите ПостроитьИндекс().")?;
        let id_field = engine.id_field.unwrap();
        let text_field = engine.text_field.unwrap();
        let extra_field = engine.extra_field.unwrap();

        let mut writer: tantivy::IndexWriter<TantivyDocument> = index.writer(50_000_000)?;
        writer.add_document(doc!(
            id_field => id.to_string(),
            text_field => text.to_string(),
            extra_field => extra.to_string()
        ))?;
        writer.commit()?;
        drop(writer);

        engine.total += 1;
        Ok(1)
    }

    fn do_remove(&self, id: &str) -> Result<i32, Box<dyn std::error::Error>> {
        let mut engine = self.engine.lock().unwrap();

        let index = engine
            .index
            .as_ref()
            .ok_or("Индекс не построен. Сначала вызовите ПостроитьИндекс().")?;
        let id_field = engine.id_field.unwrap();

        let mut writer: tantivy::IndexWriter<TantivyDocument> = index.writer(50_000_000)?;
        let term = tantivy::Term::from_field_text(id_field, id);
        writer.delete_term(term);
        writer.commit()?;
        drop(writer);

        engine.total = engine.total.saturating_sub(1);
        Ok(1)
    }
}
