//
//  dbExtract.swift
//  Business Applications
//
//  Created by Syed Ibrahim on 6/6/18.
//  Copyright © 2019 Syed Ibrahim. All rights reserved.
//


//MARK: - Database Functions

let SQLITE_STATIC = unsafeBitCast(0, to: sqlite3_destructor_type.self)
let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

var databaseResult: Int32 = 0
var databaseFile: String = ""
var databaseMessage: [String] = []

func dbInit(databaseFileName: String = "appDB.db") {
  databaseFile = (FileManager.default.urls(
    for:.documentDirectory, in:.userDomainMask)[0])
    .appendingPathComponent(databaseFileName).path
  
  if !databaseFile.isEmpty {
    if let databaseFileURL = URL(string: databaseFile) {
      databaseMessage.append((fileExists(file: databaseFileURL)) ?
        "database file: \(databaseFile)" : "database file not found!")
    }
  }
}

func dbOpen() -> OpaquePointer? {
  var db: OpaquePointer? = nil
  
  if databaseFile.isEmpty {
    dbInit()
  }
  
  databaseResult = sqlite3_open(databaseFile, &db)
  if databaseResult != SQLITE_OK {
    db = nil
  }
  
  return db
}

func dbGetCode() -> Int32 {
  return databaseResult
}

func dbGetMessage() -> [String] {
  return databaseMessage
}

func dbCreateTables(tableValuesArray: [String]) -> Bool {
  var result: Bool = false
  if let db = dbOpen() {
    result = true
    for i in 0 ..< tableValuesArray.count {
      if sqlite3_exec(db, "CREATE TABLE IF NOT EXISTS " +
        tableValuesArray[i], nil, nil, nil) != SQLITE_OK {
        result = false
        databaseMessage.append("DB CREATE FAILED: \(tableValuesArray[i])")
      }
    }
  }
  return result
}

func dbDeleteTables(tablesArray: [String]) -> Bool {
  var result: Bool = false
  if let db = dbOpen() {
    result = true
    for i in 0 ..< tablesArray.count {
      if sqlite3_exec(db, "DROP TABLE IF EXISTS " +
        tablesArray[i], nil, nil, nil) != SQLITE_OK {
        result = false
      }
    }
  }
  return result
}

func dbDelete(tableName: String,
              conditions: [String],
              conditionOr: Bool = false) -> Int32 {
  
  return dbUpdate(tableName: tableName,
                  columns: [""], values: [],
                  conditionValues: conditions,
                  conditionOr: conditionOr, delete: true)
}

func dbSetValue(tableName: String,
                column: String,
                value: String,
                type: String = "text",
                condition: String = "id=0") -> Int32 {
  
  return dbUpdate(tableName: tableName,
                  columns: [column],
                  values: [[[type, value]]],
                  conditionValues: [condition])
}


// usage notes: same as dbInsert as data-insertion is
// a fall-through if record to be updated does not exist

func dbUpdate(tableName: String,
              columns: [String],
              values: [[[String]]],
              conditionValues: [String],
              conditionOr: Bool = false,
              insert: Bool = true,
              delete: Bool = false) -> Int32 {
  
  var updateCount: Int32 = 0
  
  if !tableName.trim().isEmpty
    && conditionValues.count > 0
    && (columns.count > 0 || delete) {
    if let db = dbOpen() {
      var query: OpaquePointer?
      var columnValueString: String = ""
      var conditionString: String = ""
      var keyValue: [String] = []
      let conditionConjunction: String = (conditionOr) ? " OR " : " AND "
      
      if !delete {
        for c in 0 ..< columns.count {
          //if !columns[c].trim().isEmpty && !values[0][c][1].trim().isEmpty {
          if !columns[c].trim().isEmpty {
            columnValueString += "\(columns[c].trim())='\(values[0][c][1].trim())',"
          }
        }
        if !columnValueString.isEmpty {
          columnValueString = columnValueString.rightTrim(1)
        }
      }
      
      for i in 0 ..< conditionValues.count {
        keyValue = conditionValues[i].components(separatedBy: "=")
        if keyValue.count == 2 {
          if !keyValue[0].trim().isEmpty && !keyValue[1].trim().isEmpty {
            conditionString += "\(keyValue[0].trim())='\(keyValue[1].trim())'\(conditionConjunction)"
          }
        }
      }
      conditionString = conditionString.rightTrim(conditionConjunction.count)
      
      let queryString = (delete) ?
        "DELETE FROM \(tableName) WHERE \(conditionString)" :
      "UPDATE \(tableName) SET \(columnValueString) WHERE \(conditionString)"
      
      let databaseMessageClause = (delete) ? "deleted!" : "updated!"
      
      if sqlite3_prepare_v2(db, queryString, -1, &query, nil) == SQLITE_OK {
        if sqlite3_step(query) == SQLITE_DONE {
          updateCount = sqlite3_changes(db)
          if updateCount > 0 {
            databaseMessage.append("database update: \(updateCount) record(s) \(databaseMessageClause)")
          } else {
            databaseMessage.append("database update: record not \(databaseMessageClause)")
            if !delete {
              updateCount = dbInsert(tableName: tableName, columns: columns, values: values)
            }
          }
        } else {
          databaseMessage.append("database update: execution error!")
        }
      } else {
        databaseMessage.append("database update: query preparation error!")
      }
      
      sqlite3_finalize(query)
      sqlite3_close(db)
    }
  }
  
  print (databaseMessage)
  
  return updateCount
}


// usage notes:
//
// columns:
// let insertColumns: [String] = ["column-1-Name", "column-2-Name", "column-n-Name",...]
//
// values:
// let insertValues: [[[String]]] =
//   [[["fieldType", 1stInsert-column-1-Value],
//   ["fieldType", 1stInsert-column-2-Value],
//   ["fieldType", 1stInsert-column-n-Value]...],
//   [["blob", 2ndInsert-column-1-Value],
//   ["text", 2ndInsert-column-2-Value],
//   ["number", 2ndInsert-column-n-Value]...],
//   [["blob", nthInsert-column-1-Value],
//   ["number", nthInsert-column-2-Value],
//   ["text", nthInsert-column-n-Value]...]...]

func dbInsert(tableName: String,
              columns: [String],
              values: [[[String]]]) -> Int32 {
  
  var insertCount: Int32 = 0
  var insertPos: Int64 = 0
  
  if !tableName.trim().isEmpty && columns.count > 0 {
    if let db = dbOpen() {
      var query: OpaquePointer?
      var bindError: Bool = false
      var columnString: String = ""
      var valuePlaceholder: String = ""
      for i in 0 ..< columns.count {
        columnString += columns[i] + ","
        valuePlaceholder += "?,"
      }
      columnString = String(columnString.rightTrim(1))
      valuePlaceholder = String(valuePlaceholder.rightTrim(1))
      
      for v in 0 ..< values.count {
        let queryString = "INSERT INTO \(tableName) (\(columnString)) VALUES (\(valuePlaceholder))"
        if sqlite3_prepare_v2(db, queryString, -1, &query, nil) == SQLITE_OK {
          for c in 0 ..< columns.count {
            switch values[v][c][0] {
            case "text":
              databaseResult =
                sqlite3_bind_text(query, Int32(c + 1), values[v][c][1],
                                  Int32(values[v][c][1].count), SQLITE_TRANSIENT)
              if databaseResult != SQLITE_OK {
                bindError = true
                databaseMessage.append("database insert: text bind error!")
                break
              }
            case "number":
              databaseResult = sqlite3_bind_int(query, Int32(c + 1), Int32(values[v][c][1])!)
              if databaseResult != SQLITE_OK {
                bindError = true
                databaseMessage.append("database insert: integer bind error!")
                break
              }
            case "decimal":
              databaseResult = sqlite3_bind_double(query, Int32(c + 1), Double(values[v][c][1])!)
              if databaseResult != SQLITE_OK {
                bindError = true
                databaseMessage.append("database insert: decimal bind error!")
                break
              }
            case "blob":
              let blobBytes = [UInt8]((values[v][c][1].data(using: .utf8))!)
              databaseResult =
                sqlite3_bind_blob(query, Int32(c + 1), blobBytes,
                                  Int32(blobBytes.count), SQLITE_TRANSIENT)
              if databaseResult != SQLITE_OK {
                databaseMessage.append("database insert: blob bind error! \(c + 1)/\(v)")
                bindError = true
                break
              }
            default:
              databaseMessage.append("database insert: data type missing!")
            }
          }
          
          if !bindError {
            if sqlite3_step(query) == SQLITE_DONE {
              insertPos = sqlite3_last_insert_rowid(db)
              insertCount = sqlite3_changes(db)
              databaseMessage.append("database insert: data inserted at ID: \(insertPos)")
            }
          }
        }
      }
      sqlite3_finalize(query)
      sqlite3_close(db)
    }
  }
  
  return insertCount
}

func dbGetValue(tableName: String,
                column: String,
                type: String = "text",
                conditionValues: [String] = ["id=1"],
                conditionOr: Bool = true) -> String {
  
  var result = ""
  let dbData = dbRead(tableName: tableName,
                      columns: [[type, column]],
                      conditionValues: conditionValues,
                      conditionOr: conditionOr)
  
  if dbData.count > 0 {
    result = dbData[0][column]!
  }
  
  return result
}

func dbRead(tableName: String,
            columns: [[String]],
            conditionValues: [String] = [],
            conditionOr: Bool = false) -> [[String:String]] {
  
  var result: [[String:String]] = []
  
  if !tableName.trim().isEmpty && columns.count > 0 {
    if let db = dbOpen() {
      var query: OpaquePointer?
      var columnString: String = ""
      var conditionString: String = ""
      var keyValue: [String] = []
      let conditionConjunction: String = (conditionOr) ? " OR " : " AND "
      
      for i in 0 ..< columns.count {
        columnString += columns[i][1] + ","
      }
      columnString = columnString.rightTrim(1)
      
      if conditionValues.count > 0 {
        conditionString = " WHERE "
        for i in 0 ..< conditionValues.count {
          keyValue = conditionValues[i].components(separatedBy: "=")
          if keyValue.count == 2 {
            if !keyValue[0].trim().isEmpty && !keyValue[1].trim().isEmpty {
              conditionString += "\(keyValue[0].trim())='\(keyValue[1].trim())'\(conditionConjunction)"
            }
          }
        }
        conditionString = conditionString.rightTrim(conditionConjunction.count)
      }
      
      let queryString = "SELECT \(columnString) FROM \(tableName) \(conditionString)"
      databaseResult = sqlite3_prepare_v2(db, queryString, -1, &query, nil)
      
      if databaseResult == SQLITE_OK {
        var row: [String:String] = [:]
        while sqlite3_step(query) == SQLITE_ROW {
          for i in 0 ..< columns.count {
            let queryLength = sqlite3_column_bytes(query, Int32(i))
            switch columns[i][0] {
            case "text":
              let queryResult = sqlite3_column_text(query, Int32(i))
              row[columns[i][1]] = (queryResult == nil) ? "" : String(cString: queryResult!)
            case "number":
              let queryResult = sqlite3_column_int(query, Int32(i))
              // sqlite3_column_int automatically returns zero on null values
              row[columns[i][1]] = String(queryResult)
            case "blob":
              let queryResult = sqlite3_column_blob(query, Int32(i))
              let queryData = (queryResult == nil) ? NSData() : NSData(bytes: queryResult, length: Int(queryLength))
              row[columns[i][1]] = NSString(data: queryData as Data,
                                            encoding: String.Encoding.utf8.rawValue)! as String
            default:
              databaseMessage.append("database read: data type missing!")
            }
          }
          result.append(row)
        }
      } else {
        databaseMessage.append("database read: error preparing query!")
      }
      sqlite3_finalize(query)
      sqlite3_close(db)
    }
  }
  return result
}
