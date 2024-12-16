use std::io::{stdin, stdout, Write};

use aws_config::{self, Region};
use aws_sdk_dynamodb::{
    self as ddb,
    types::{AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType},
    Client,
};
use dotenvy;

// IMPORTANT NOTE: An AWS account is required. A user with suitable permissions is also required (set up in IAM)
#[tokio::main]
async fn main() {
    #[cfg(debug_assertions)]
    {
        // Load the .envlocal file (or .env by default) for local development
        // If deployed, use platform environment variables
        dotenvy::from_filename(".envlocal").expect(".env file not found");
    }

    let region = dotenvy::var("AWS_REGION").expect("AWS_REGION must be set in a .env file");
    // AWS looks for AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables
    // so no need to explicitly assign them to variables
    let sdk_config = aws_config::from_env()
        .region(Region::new(region.clone()))
        .load()
        .await;
    // New DynamoDB client instance
    let ddb_client = Client::new(&sdk_config);
    // Table name
    let table_name = "stratusgrid-products";

    let attr_part = AttributeDefinition::builder()
        .attribute_name("category")
        .attribute_type(ScalarAttributeType::S)
        .build()
        .unwrap();

    let attr_sort = AttributeDefinition::builder()
        .attribute_name("productname")
        .attribute_type(ScalarAttributeType::S)
        .build()
        .unwrap();

    let keyschema_part = KeySchemaElement::builder()
        .attribute_name("category")
        .key_type(KeyType::Hash)
        .build()
        .unwrap();

    let keyschema_sort = KeySchemaElement::builder()
        .attribute_name("productname")
        .key_type(KeyType::Range)
        .build()
        .unwrap();

    let create_result = ddb_client
        .create_table()
        .table_name(table_name)
        .billing_mode(ddb::types::BillingMode::PayPerRequest)
        .attribute_definitions(attr_part)
        .attribute_definitions(attr_sort)
        .key_schema(keyschema_part)
        .key_schema(keyschema_sort)
        .send()
        .await;

    if create_result.is_ok() {
        println!("Creating DynamoDB table was successful!");
    } else {
        println!("Error occurred while creating DynamoDB table.");
        println!("{:#?}", create_result.err());
    }

    loop {
        let new_category = get_value("category".to_string());
        if new_category == "q".to_string() {
            break;
        }
        let new_productname = get_value("product name".to_string());
        let new_price = get_value("price".to_string());

        write_product(
            &ddb_client,
            table_name.to_string(),
            new_category,
            new_productname,
            new_price,
        )
        .await;
    }
}

async fn write_product(
    ddb_client: &Client,
    table_name: String,
    category: String,
    productname: String,
    price: String,
) {
    let put_item_result = ddb_client
        .put_item()
        .table_name(table_name)
        .item("category", AttributeValue::S(category))
        .item("productname", AttributeValue::S(productname))
        .item("price", AttributeValue::N(price))
        .send()
        .await;

    if put_item_result.is_err() {
        println!("{:#?}", put_item_result.err());
    }
}

fn get_value(value: String) -> String {
    print!("Enter {}: ", value);
    _ = stdout().flush();
    let mut user_input = String::new();
    _ = stdin().read_line(&mut user_input);
    user_input = user_input.trim_end().to_string();

    return user_input;
}
