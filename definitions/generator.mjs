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
function generateHub(table_name, business_key, source_table_AI, source_table_SJ) {
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
`.trim();
}

// --- AI SATELLITE GENERATOR ---
function generateSatellite_AI(table_name, business_key, descriptive_fields, source_table_AI) {
  const attributes = descriptive_fields
    .split('\n')
    .map(attr => attr.trim())
    .filter(attr => attr.length > 0);

  const attrSelect = attributes.join(',\n  ');
  const attrGroup = attributes.join(', ');

  return `
config {
  type: "table",
  schema: "raw_vault",
  tags: ["satellite"]
}

SELECT
  MD5(${business_key}) AS HK_${business_key},
  ${attrSelect},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_AI}' AS REC_SRC
FROM \${ref("${source_table_AI}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}${attrGroup ? ', ' + attrGroup : ''}
`.trim();
}

// --- SJ SATELLITE GENERATOR ---
function generateSatellite_SJ(table_name, business_key, descriptive_fields, source_table_SJ) {
  const attributes = descriptive_fields
    .split('\n')
    .map(attr => attr.trim())
    .filter(attr => attr.length > 0);

  const attrSelect = attributes.join(',\n  ');
  const attrGroup = attributes.join(', ');

  return `
config {
  type: "table",
  schema: "raw_vault",
  tags: ["satellite"]
}

SELECT
  MD5(${business_key}) AS HK_${business_key},
  ${attrSelect},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_SJ}' AS REC_SRC
FROM \${ref("${source_table_SJ}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}${attrGroup ? ', ' + attrGroup : ''}
`.trim();
}

// --- MAIN LOOP ---
records.forEach(row => {
  if (row.table_type === 'HUB') {
    const script = generateHub(
      row.table_name,
      row.business_key,
      row.source_table_AI,
      row.source_table_SJ
    );
    const fileName = `HUB_${row.table_name.toUpperCase()}.sqlx`;
    const filePath = path.join(targetDir, fileName);
    fs.writeFileSync(filePath, script);
    console.log(`✅ HUB SQLX file '${filePath}' has been created.`);
  } else if (row.table_type === 'SAT') {
    // Generate AI Satellite
    const scriptAI = generateSatellite_AI(
      row.table_name,
      row.business_key,
      row.descriptive_fields,
      row.source_table_AI
    );
    const fileNameAI = `SAT_${row.table_name.toUpperCase()}.sqlx`;
    const filePathAI = path.join(targetDir, fileNameAI);
    fs.writeFileSync(filePathAI, scriptAI);
    console.log(`✅ SAT SQLX file '${filePathAI}' has been created.`);

    // Generate SJ Satellite
    const scriptSJ = generateSatellite_SJ(
      row.table_name,
      row.business_key,
      row.descriptive_fields,
      row.source_table_SJ
    );
    const fileNameSJ = `SAT_${row.table_name.toUpperCase()}.sqlx`;
    const filePathSJ = path.join(targetDir, fileNameSJ);
    fs.writeFileSync(filePathSJ, scriptSJ);
    console.log(`✅ SAT SQLX file '${filePathSJ}' has been created.`);
  }
});