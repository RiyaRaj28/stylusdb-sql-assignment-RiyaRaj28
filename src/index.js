const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinTable, joinCondition } = parseQuery(query);
    // console.log("hii", fields, table, whereClauses, joinTable, joinCondition);

    let data = await readCSV(`${table}.csv`);

    if(joinTable && joinCondition){
        const joinData = await readCSV(`${joinTable}.csv`);
        data = data.flatMap(mainRow => {
            return joinData
            .filter(joinRow => 
            {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                // console.log("mainVal", mainValue);
                // console.log("joinValuee", joinValue);
                return mainValue === joinValue; 
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc; 
                }, {});
            });
        });
        // console.log("joinrow, joindata", joinRow, joinData);

        const filteredData = whereClauses.length > 0 
        ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
        : data;
        console.log("hii", filteredData);

        return filteredData.map(row => {
            const selectedRow = {};
            fields.forEach(field => {
                selectedRow[field] = row[field];
                console.log("hiii these are filtered data", selectedRow);  

            });
        return selectedRow; 
        });


    }
}
module.exports = executeSELECTQuery;