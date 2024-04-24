function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        // console.log("matched statement", match);
        if (match) {
            const [, field, operator, value] = match;
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);
    // console.log("joinMatch", joinMatch);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}


function parseQuery(query) {
    query = query.trim();
    let selectPart, joinPart; 

    const whereSplit = query.split(/\sWHERE\s/i);
    //SELECT student.name, enrollment.course FROM student INNER JOIN enrollment ON student.id=enrollment.student_id 
    query = whereSplit[0];

    const whereClause = whereSplit.length>1 ? whereSplit[1].trim() : null;    //assigned empty array here
    //student.name = John

    const joinSplit = query.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
    selectPart = joinSplit[0].trim(); 
  
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;         //have
    const selectMatch = selectPart.match(selectRegex);
    if(!selectMatch){
        throw new Error('Invalid SELECT format');
    }
    const[, fields , table] = selectMatch;  


    const{ joinType, joinTable, joinCondition } = parseJoinClause(query); 

    let whereClauses = [];
    if(whereClause){
        whereClauses = parseWhereClause(whereClause); 
    }
    // console.log("wherecheck", whereClauses);

    // console.log("fields, table,whereClauses,joinType,joinTable,joinCondition", fields, table,whereClauses,joinType,joinTable,joinCondition);

    return{
        fields : fields.split(',').map(field => field.trim()),
        table : table.trim(),
        whereClauses,
        joinType,
        joinTable,
        joinCondition,
    }; 

}

module.exports = { parseQuery, parseJoinClause };



















