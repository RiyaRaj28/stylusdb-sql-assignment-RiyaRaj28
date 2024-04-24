const { parseQuery, parseJoinClause } = require('./queryParser');
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

function performInnerJoin(data, joinData, joinCondition, fields, table){
    data = data.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table 
                    ? mainRow[fieldName] 
                    : joinRow[fieldName];

                    return acc;
                }, {});
            });
    });
    return data;
}

function performLeftJoin(data, joinData, joinCondition, fields, table){
    let joinValue; 
 
    let matchingdata = data.flatMap(mainRow => {
            return joinData
            .filter(joinRow => {
                let mainValue = mainRow[joinCondition.left.split('.')[1]];         //id
                joinValue = joinRow[joinCondition.right.split('.')[1]];        //student_id
                return mainValue === joinValue; 
            })
            .map(joinRow => {
             return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table 
                    ? mainRow[fieldName] 
                    : joinRow[fieldName];
    
                    return acc;
                }, {});
            
            });
        
    });

    let nonMatchingdata = {};
    let count;

    for(let i=0; i<data.length; i++)
    {
        count = 0; 
        const currData = data[i];
        for(let j=0; j<joinData.length; j++)
        {
            const joindata = joinData[j];
            if(currData[joinCondition.left.split('.')[1]] == joindata[joinCondition.right.split('.')[1]])
            {
                count += 1; 
            }
        }

        if(count == 0)
        {
            nonMatchingdata = data[i]; 
        }
    }

    const resultArray = fields.reduce((acc, field) => {
        const[tableName, fieldName] = field.split('.');

        acc[field] = tableName === table ? nonMatchingdata[fieldName] : null; 
        return acc;
    }, {});

    matchingdata.push(resultArray)
    return matchingdata; 
}

function performRightJoin(data, joinData, joinCondition, fields, table){

    let joinValue; 
 
    let matchingdata = data.flatMap(mainRow => {
            return joinData
            .filter(joinRow => {
                let mainValue = mainRow[joinCondition.left.split('.')[1]];         //id
                joinValue = joinRow[joinCondition.right.split('.')[1]];        //student_id.
                return mainValue === joinValue; 
            })
            .map(joinRow => {
             return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table 
                    ? mainRow[fieldName] 
                    : joinRow[fieldName];
    
                    return acc;
                }, {});
            });
    });

    let nonMatchingdata = {};
    let count;

    for(let i=0; i<joinData.length; i++)
    {
        count = 0; 
        const joindata = joinData[i];
        for(let j=0; j<data.length; j++)
        {
            const currdata = data[j];
            if(joindata[joinCondition.right.split('.')[1]] == currdata[joinCondition.left.split('.')[1]])
            {
                count += 1; 
            }
        }

        if(count == 0)
        {
            nonMatchingdata = joinData[i]; 
        }
    }

    const resultArray = fields.reduce((acc, field) => {
        const[tableName, fieldName] = field.split('.');

        acc[field] = tableName === table ? null : nonMatchingdata[fieldName]; 
        return acc;
    }, {});

    matchingdata.push(resultArray)

    return matchingdata; 
}

async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinType, joinTable, joinCondition } = parseQuery(query);
  
    let data = await readCSV(`${table}.csv`);

    if(joinTable && joinCondition){
        const joinData = await readCSV(`${joinTable}.csv`);
        switch(joinType.toUpperCase()){
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break; 
            default :
              throw new Error("Unsupported join types");
        }
    }


    const filteredData = whereClauses.length > 0
    ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
    : data ;

    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });

}

module.exports = executeSELECTQuery;