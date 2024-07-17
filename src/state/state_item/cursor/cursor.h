#pragma once

#include "record/record.h"

class AbstractState {
public:
};

class IndexScanState: public AbstractState {

};

class SeqScanState: public AbstractState {

};

class NestedLoopState: public AbstractState {

};

class SortState: public AbstractState {

};

class InsertState: public AbstractState {
    
};

class DeleteState: public AbstractState {

};

class UpdateState: public AbstractState {

};

class ProjectionPlan: public AbstractState {

};

