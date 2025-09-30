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
  fs.mkdirSync(targetDir);
}

function generateHub(table_name, table_type, business_key, source_table_AI, source_table_SJ) {
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
  '${source_table_AI}' AS REC_SRC
FROM \${ref("${source_table_AI}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}
UNION ALL
SELECT
  MD5(${business_key}) AS HK_${business_key},
  ${business_key},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_SJ}' AS REC_SRC
FROM \${ref("${source_table_SJ}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}
`;
}

records.forEach(hub => {
  const script = generateHub(hub.table_name, hub.table_type, hub.business_key, hub.source_table_AI, hub.source_table_SJ);
  const fileName = `${hub.table_type}_${hub.table_name.toUpperCase()}.sqlx`;
  const filePath = path.join(targetDir, fileName);
  fs.writeFileSync(filePath, script);
  console.log(`✅ SQLX file '${filePath}' has been created.`);
});