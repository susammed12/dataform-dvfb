
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { parse } from 'csv-parse/sync';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const csvPath = path.join(__dirname, 'metadata.csv');
const csvContent = fs.readFileSync(csvPath, 'utf-8');

const records = parse(csvContent, {
  columns: true,
  skip_empty_lines: true
});

const targetDir = path.join(__dirname, '../definitions/rv_generator');
if (!fs.existsSync(targetDir)) {
  fs.mkdirSync(targetDir, { recursive: true });
}

// --- HUB GENERATOR ---
function generateHub(table_name, business_key, source_table) {
  return `
config {
  type: "table",
  schema: "raw_vault",
  tags: ["hub"]
}

SELECT
  MD5(${business_key}) AS HK_${business_key},
  ${business_key},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table}' AS REC_SRC
FROM \${ref("${source_table}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}
`.trim();
}

// --- SATELLITE GENERATOR ---
function generateSatellite(table_name, business_key, descriptive_fields, source_table) {
  const attributes = (descriptive_fields || '')
    .split('|')
    .map(attr => attr.trim())
    .filter(attr => attr.length > 0);

  const attrSelect = attributes.join(',\n  ');
  const attrGroup = attributes
    .map(attr => `COALESCE(CAST(${attr} AS STRING), '')`)
    .join(", '|', ");

  return `
{{ config(
  materialized = "incremental",
  schema = "raw_vault",
  tags = ["satellite"]
) }}

-- Step 1: Mark previous records as not current
{% if is_incremental() %}
UPDATE {{ this }}
SET _is_current = FALSE
WHERE HK_${business_key} IN (
  SELECT MD5(${business_key})
  FROM {{ ref("${source_table}") }}
  WHERE ${business_key} IS NOT NULL
);
{% endif %}

-- Step 2: Insert new records with _is_current = TRUE
SELECT
  MD5(${business_key}) AS HK_${business_key},
  MD5(CONCAT(${attrGroup})) AS HASHDIFF,
  ${attrSelect},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table}' AS REC_SRC,
  TRUE AS _is_current
FROM {{ ref("${source_table}") }}
WHERE ${business_key} IS NOT NULL
`.trim();
}

// --- MAIN LOOP ---
records.forEach(row => {
  if (row.table_type === 'HUB') {
    const script = generateHub(
      row.table_name,
      row.business_key,
      row.source_table
    );
    const fileName = `HUB_${row.table_name.toUpperCase()}.sqlx`;
    const filePath = path.join(targetDir, fileName);
    fs.writeFileSync(filePath, script);
    console.log(`✅ HUB SQLX file '${filePath}' has been created.`);

  } else if (row.table_type === 'SAT') {
    const script = generateSatellite(
      row.table_name,
      row.business_key,
      row.descriptive_fields,
      row.source_table
    );
    const fileName = `SAT_${row.table_name.toUpperCase()}_test.sqlx`;
    const filePath = path.join(targetDir, fileName);
    fs.writeFileSync(filePath, script);
    console.log(`✅ SAT AI SQLX file '${filePath}' has been created.`);

  } 
});
