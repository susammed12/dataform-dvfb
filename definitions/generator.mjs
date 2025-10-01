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
    const fileNameAI = `SAT_AI_${row.table_name.toUpperCase()}.sqlx`;
    const filePathAI = path.join(targetDir, fileNameAI);
    fs.writeFileSync(filePathAI, scriptAI);
    console.log(`✅ SAT_AI SQLX file '${filePathAI}' has been created.`);

    // Generate SJ Satellite
    const scriptSJ = generateSatellite_SJ(
      row.table_name,
      row.business_key,
      row.descriptive_fields,
      row.source_table_SJ
    );
    const fileNameSJ = `SAT_SJ_${row.table_name.toUpperCase()}.sqlx`;
    const filePathSJ = path.join(targetDir, fileNameSJ);
    fs.writeFileSync(filePathSJ, scriptSJ);
    console.log(`✅ SAT_SJ SQLX file '${filePathSJ}' has been created.`);
  }
});