import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const metadata = {
  "hubs": [
    {
      "table_name": "FLIGHT",
      "business_key": "FlightID",
      "source_table_AI": "AI_FLIGHT_DETAILS",
      "source_table_SJ": "SJ_FLIGHT_DETAILS"
    },
    {
      "table_name": "AIRPORT",
      "business_key": "AirportCode",
      "source_table_AI": "AI_AIRPORT_DETAILS",
      "source_table_SJ": "SJ_AIRPORT_DETAILS"
    }
  ]
};

// Ensure the target directory exists
const targetDir = path.join(__dirname, 'Test_generator');
if (!fs.existsSync(targetDir)) {
  fs.mkdirSync(targetDir);
}

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
GROUP BY ${business_key};
`;
}

metadata.hubs.forEach(hub => {
  const script = generateHub(hub.table_name, hub.business_key, hub.source_table_AI, hub.source_table_SJ);
  const fileName = `${hub.table_name.toLowerCase()}.sqlx`;
  const filePath = path.join(targetDir, fileName);
  fs.writeFileSync(filePath, script);
  console.log(`✅ SQLX file '${filePath}' has been created.`);
});
