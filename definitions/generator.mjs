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
  bigquery: { partitionBy: "DATE(LOAD_DTS)", clusterBy: ["HK_${business_key}"] },
  schema: "raw_vault",
  tags: ["hub"]
}

SELECT
  TO_HEX(MD5(${business_key})) AS HK_${business_key},
  ${business_key},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_AI}' AS REC_SRC
FROM \${ref("${source_table_AI}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}
UNION ALL
SELECT
  TO_HEX(MD5(${business_key})) AS HK_${business_key},
  ${business_key},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_SJ}' AS REC_SRC
FROM \${ref("${source_table_SJ}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}
`.trim();
}

// --- AI SATELLITE GENERATOR ---
function generateSatellite_AI(table_name, business_key, descriptive_fields_AI, source_table_AI) {
  const attributes = (descriptive_fields_AI || '')
    .split('|')
    .map(attr => attr.trim())
    .filter(attr => attr.length > 0);

  const attrSelect = attributes.join(',\n  ');
  const attrGroup = attributes.join(', ');

  return `
config {
  type: "table",
  bigquery: { partitionBy: "DATE(LOAD_DTS)", clusterBy: ["HK_${business_key}"] },
  schema: "raw_vault",
  tags: ["satellite"]
}

SELECT
  TO_HEX(MD5(${business_key})) AS HK_${business_key},
  ${attrSelect},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_AI}' AS REC_SRC
FROM \${ref("${source_table_AI}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}${attrGroup ? ', ' + attrGroup : ''}
`.trim();
}

// --- SJ SATELLITE GENERATOR ---
function generateSatellite_SJ(table_name, business_key, descriptive_fields_SJ, source_table_SJ) {
  const attributes = (descriptive_fields_SJ || '')
    .split('|')
    .map(attr => attr.trim())
    .filter(attr => attr.length > 0);

  const attrSelect = attributes.join(',\n  ');
  const attrGroup = attributes.join(', ');

  return `
config {
  type: "table",
  bigquery: { partitionBy: "DATE(LOAD_DTS)", clusterBy: ["HK_${business_key}"] },
  schema: "raw_vault",
  tags: ["satellite"]
}

SELECT
  TO_HEX(MD5(${business_key})) AS HK_${business_key},
  ${attrSelect},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_SJ}' AS REC_SRC
FROM \${ref("${source_table_SJ}")}
WHERE ${business_key} IS NOT NULL
GROUP BY ${business_key}${attrGroup ? ', ' + attrGroup : ''}
`.trim();
}

// --- LINK GENERATOR ---
function generateLink(table_name, business_key, source_table_AI, source_table_SJ) {
  const keys = business_key.split('|').map(k => k.trim()).filter(k => k.length > 0);
  const md5EachKey = keys.map(k => `TO_HEX(MD5(${k})) AS HK_${k}`).join(',\n  ');
  const hashKey = `HK_L_${table_name.toUpperCase()}`;
  const hashExpression = keys.map(k => `COALESCE(${k}, '')`).join(" || '|' || ");
  const notNullConditions = keys.map(k => `${k} IS NOT NULL`).join(' AND ');

  return `
config {
  type: "table",
  bigquery: { partitionBy: "DATE(LOAD_DTS)", clusterBy: ["${hashKey}"] },
  schema: "raw_vault",
  tags: ["link"]
}

SELECT
  TO_HEX(MD5(${hashExpression})) AS ${hashKey},
  ${md5EachKey},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_AI}' AS REC_SRC
FROM \${ref("${source_table_AI}")}
WHERE ${notNullConditions}

UNION ALL

SELECT
  TO_HEX(MD5(${hashExpression})) AS ${hashKey},
  ${md5EachKey},
  CURRENT_TIMESTAMP() AS LOAD_DTS,
  '${source_table_SJ}' AS REC_SRC
FROM \${ref("${source_table_SJ}")}
WHERE ${notNullConditions}
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
    const scriptAI = generateSatellite_AI(
      row.table_name,
      row.business_key,
      row.descriptive_fields_AI,
      row.source_table_AI
    );
    const fileNameAI = `SAT_${row.table_name.toUpperCase()}_AI.sqlx`;
    const filePathAI = path.join(targetDir, fileNameAI);
    fs.writeFileSync(filePathAI, scriptAI);
    console.log(`✅ SAT AI SQLX file '${filePathAI}' has been created.`);

    const scriptSJ = generateSatellite_SJ(
      row.table_name,
      row.business_key,
      row.descriptive_fields_SJ,
      row.source_table_SJ
    );
    const fileNameSJ = `SAT_${row.table_name.toUpperCase()}_SJ.sqlx`;
    const filePathSJ = path.join(targetDir, fileNameSJ);
    fs.writeFileSync(filePathSJ, scriptSJ);
    console.log(`✅ SAT SJ SQLX file '${filePathSJ}' has been created.`);

  } else if (row.table_type === 'LINK') {
    const script = generateLink(
      row.table_name,
      row.business_key,
      row.source_table_AI,
      row.source_table_SJ
    );
    const fileName = `LINK_${row.table_name.toUpperCase()}.sqlx`;
    const filePath = path.join(targetDir, fileName);
    fs.writeFileSync(filePath, script);
    console.log(`✅ LINK SQLX file '${filePath}' has been created.`);
  }
});