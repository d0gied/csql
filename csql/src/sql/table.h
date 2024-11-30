#pragma once

#include <stdio.h>

#include <vector>

#include "expr.h"

namespace csql {

struct SelectStatement;
struct TableRef;

// Possible table reference types.
enum TableRefType { kTableName, kTableSelect, kTableJoin, kTableCrossProduct };

}  // namespace csql
