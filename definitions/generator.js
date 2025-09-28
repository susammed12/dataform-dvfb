const metadata = {
  "hubs": [
    {
      "entity": "Passenger",
      "business_key": "PassengerID",
      "source_table": "staging_passenger"
    },
    {
      "entity": "Flight",
      "business_key": "FlightID",
      "source_table": "staging_flight"
    }
  ],
  "links": [
    {
      "link_name": "Booking",
      "keys": [
        "PassengerID",
        "FlightID",
        "PaymentID"
      ],
      "source_table": "staging_booking"
    }
  ],
  "satellites": [
    {
      "satellite_name": "PassengerDetails",
      "parent_key": "PassengerID",
      "attributes": [
        "FirstName",
        "LastName",
        "DOB",
        "Email"
      ],
      "source_table": "staging_passenger"
    }
  ]
};

function generateHub(entity, businessKey, sourceTable) {
  return `
config {
  type: "table",
  tags: ["hub"]
}

CREATE OR REPLACE TABLE \`project.dataset.hub_${entity.toLowerCase()}\` AS
SELECT
  MD5(${businessKey}) AS Hub_${entity}_Key,
  ${businessKey},
  CURRENT_TIMESTAMP() AS LoadDate,
  '${sourceTable}' AS RecordSource
FROM \`project.dataset.${sourceTable}\`
WHERE ${businessKey} IS NOT NULL;
`;
}

function generateLink(linkName, keys, sourceTable) {
  const linkKey = keys.map(k => `CAST(${k} AS STRING)`).join(", ");
  const hubKeys = keys.map(k => `MD5(${k}) AS Hub_${k}_Key`).join(",\n  ");
  return `
config {
  type: "table",
  tags: ["link"]
}

CREATE OR REPLACE TABLE \`project.dataset.link_${linkName.toLowerCase()}\` AS
SELECT
  MD5(CONCAT(${linkKey})) AS Link_${linkName}_Key,
  ${hubKeys},
  CURRENT_TIMESTAMP() AS LoadDate,
  '${sourceTable}' AS RecordSource
FROM \`project.dataset.${sourceTable}\`
WHERE ${keys.map(k => `${k} IS NOT NULL`).join(" AND ")};
`;
}

function generateSatellite(satName, parentKey, attributes, sourceTable) {
  const attrList = attributes.join(",\n  ");
  return `
config {
  type: "table",
  tags: ["satellite"]
}

CREATE OR REPLACE TABLE \`project.dataset.sat_${satName.toLowerCase()}\` AS
SELECT
  MD5(${parentKey}) AS Hub_${parentKey}_Key,
  ${attrList},
  CURRENT_TIMESTAMP() AS LoadDate,
  '${sourceTable}' AS RecordSource
FROM \`project.dataset.${sourceTable}\`;
`;
}

const sqlxScripts = [];

metadata.hubs.forEach(hub => {
  sqlxScripts.push(generateHub(hub.entity, hub.business_key, hub.source_table));
});

metadata.links.forEach(link => {
  sqlxScripts.push(generateLink(link.link_name, link.keys, link.source_table));
});

metadata.satellites.forEach(sat => {
  sqlxScripts.push(generateSatellite(sat.satellite_name, sat.parent_key, sat.attributes, sat.source_table));
});

sqlxScripts.forEach(script => console.log(script));