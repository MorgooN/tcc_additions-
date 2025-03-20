#include <TCC/TCC_money.h>
#include <TCC/TCC_thr_config.h>

#include <threads.h>
#include <stdlib.h>


typedef struct TCC_MoneyNotes
{
    size_t length;
    size_t capacity;
    TCC_Money* moneyArr;
    TCC_Date* dateArr;
} TCC_MoneyNotes;

typedef struct TCC_MoneyNotesStep_p
{
    TCC_Money* moneyPtr; 
    size_t startIndex;
    size_t endIndex;
    TCC_Money res;
} TCC_MoneyNotesStep_p;

// доп структуры для параллельных операций
typedef struct TCC_MaxTransactionData {
    TCC_Money maxAmount;
    size_t maxIndex;
    TCC_Date maxDate;
} TCC_MaxTransactionData;

typedef struct TCC_YearStats {
    TCC_Money total;
    TCC_Money min;
    TCC_Money max;
    size_t count;
} TCC_YearStats;

typedef struct TCC_FilterData {
    TCC_Money* resultArray;
    size_t resultCount;
    TCC_Money minAmount;
    TCC_Money maxAmount;
} TCC_FilterData;

TCC_Error TCC_moneyNotesCreate(TCC_MoneyNotes** moneyNotesPP, const size_t capacity)
{ 
    *moneyNotesPP = (TCC_MoneyNotes*) malloc(sizeof(TCC_MoneyNotes) 
        + capacity*(sizeof(TCC_Money) + sizeof(TCC_Date))); 
    if(*moneyNotesPP == NULL)
    {
        return TCC_ERROR_MEMORY_ALLOCATION;
    }
    (*moneyNotesPP)->capacity = capacity;
    (*moneyNotesPP)->length = 0; 
    (*moneyNotesPP)->moneyArr = (size_t*)  ((char *) *moneyNotesPP 
         + sizeof(TCC_MoneyNotes) );
    (*moneyNotesPP)->dateArr = (TCC_Date*) ((char *) (*moneyNotesPP)->moneyArr 
         + (*moneyNotesPP)->capacity * sizeof(*(*moneyNotesPP)->moneyArr));
    return TCC_ERROR_MISSING;
}

void TCC_moneyNotesDel(TCC_MoneyNotes** moneyNoterPP)
{
    free(*moneyNoterPP);
}

void TCC_moneyNotesInsert(TCC_MoneyNotes* moneyNotesP, const TCC_Money money, 
    const TCC_Date date)
{
    ++moneyNotesP->length;
    moneyNotesP->dateArr[moneyNotesP->length-1] = date;
    moneyNotesP->moneyArr[moneyNotesP->length-1] = money;
}

size_t TCC_moneyNotesGetCapacity(const TCC_MoneyNotes* moneyNotesP)
{
    return moneyNotesP->capacity;
}

static size_t TCC_moneyYearFindFirst(const TCC_MoneyNotes * historyPtr, const TCC_DateYear year)
{
    size_t i = 0;
    while(i < historyPtr->length && historyPtr->dateArr[i].year != year)
    {
        ++i;
    }
    return i;
}

static size_t TCC_moneyYearFindLast(const TCC_MoneyNotes * historyPtr, const TCC_DateYear year)
{
    size_t i = historyPtr->length - 1;
    while(i > 0 && historyPtr->dateArr[i].year != year)
    {
        --i;
    }
    return i;
}

static TCC_Money TCC_moneyRangeSum_s(const TCC_MoneyNotes * notesPtr, 
                            const size_t startIndex, const size_t endIndex)
{
    TCC_Money res = 0;
    for(size_t i = startIndex; i < endIndex+1; ++i)
    {
        res += notesPtr->moneyArr[i];
    }
    return res;
}

static int TCC_moneyRangeSum_p_step(void* data)
{    
    TCC_MoneyNotesStep_p* notesStepPtr = (TCC_MoneyNotesStep_p*) data;
    notesStepPtr->res = 0;
    for(size_t i = notesStepPtr->startIndex; i < notesStepPtr->endIndex+1; ++i)
    {
        notesStepPtr->res += (notesStepPtr->moneyPtr[i]);
    }
    return 0;
}

static void TCC_moneyRangeSum_p_init(const TCC_MoneyNotes * notesPtr, 
                                    const size_t startIndex, const size_t endIndex,
                                    thrd_t** threads, TCC_MoneyNotesStep_p** threadsData,
                                    const int numThreads)
{
    *threads = (thrd_t*) malloc(numThreads*sizeof(thrd_t));
    *threadsData = (TCC_MoneyNotesStep_p*) malloc(numThreads*sizeof(TCC_MoneyNotesStep_p));
    const size_t indicesPerThread = (endIndex - startIndex) / numThreads;    
    for(size_t i = 0; i < numThreads-1; ++i)
    {
        (*threadsData)[i].startIndex = startIndex + i*indicesPerThread;
        (*threadsData)[i].endIndex = startIndex + (i+1) * indicesPerThread - 1;
        (*threadsData)[i].moneyPtr = notesPtr->moneyArr;        
    }
    (*threadsData)[numThreads-1].startIndex = startIndex + (numThreads-1) * indicesPerThread;
    (*threadsData)[numThreads-1].endIndex = endIndex;
    (*threadsData)[numThreads-1].moneyPtr = notesPtr->moneyArr;  
}

static TCC_Money TCC_moneyRangeSum_p_proc(thrd_t* threads, TCC_MoneyNotesStep_p* threadsData, const int numThreads)
{
    TCC_Money res = 0;
    for(size_t i = 0; i < numThreads; ++i)
    {
        thrd_create(&threads[i], TCC_moneyRangeSum_p_step, (void*) &threadsData[i]);
    }
    for(size_t i = 0; i < numThreads; ++i)
    {
        thrd_join(threads[i], NULL);
    }
    for(size_t i = 0; i < numThreads; ++i)
    {
        res += threadsData[i].res; 
    }
    return res;
}

static int TCC_getOptimalThreadCount(void) {
    int cpuCount = TCC_getLogicalCPUCount();
    // Используем 75% доступных ядер для оптимального баланса
    return (cpuCount * 3) / 4;
}

static TCC_Money TCC_moneyRangeSum_p(const TCC_MoneyNotes * notesPtr, 
                                    const size_t startIndex, const size_t endIndex)
{
    // int numThreads = TCC_getLogicalCPUCount();
    int numThreads = 8;
    if(endIndex-startIndex < numThreads*TCC_MIN_MONEYS_PER_THREAD)
    {
        return TCC_moneyRangeSum_s(notesPtr, startIndex, endIndex);
    } 
    TCC_Money res = 0;
    thrd_t* threads;
    TCC_MoneyNotesStep_p* threadsData;
    TCC_moneyRangeSum_p_init(notesPtr, startIndex, endIndex, 
        &threads, &threadsData, numThreads);
    return TCC_moneyRangeSum_p_proc(threads, threadsData, numThreads);
}

static TCC_Money TCC_moneyRangeSum(const TCC_MoneyNotes * notesPtr, 
                            const size_t startIndex, const size_t endIndex)
{    
    #ifdef TCC_SINGLE_MODE
        #undef TCC_PARALLEL_MODE
        return TCC_moneyRangeSum_s(notesPtr, startIndex, endIndex);
    #endif
    #ifdef TCC_PARALLEL_MODE
        return TCC_moneyRangeSum_p(notesPtr, startIndex, endIndex);
    #endif
    return TCC_moneyRangeSum_s(notesPtr, startIndex, endIndex);
}

TCC_Money TCC_moneyYearSum(const TCC_MoneyNotes * notesPtr, const TCC_DateYear year)
{
    size_t startIndex = TCC_moneyYearFindFirst(notesPtr, year);
    if(startIndex == notesPtr->length)
    {
        return 0;
    }
    size_t endIndex = TCC_moneyYearFindLast(notesPtr, year); 
    return TCC_moneyRangeSum(notesPtr, startIndex, endIndex);
}

// ДОПОЛНЕНИЯ


TCC_MaxTransactionData TCC_findMaxTransaction(const TCC_MoneyNotes* notesPtr) {
    TCC_MaxTransactionData result = {0, 0, {0, 0, 0}};
    
    if(notesPtr->length == 0) return result;
    
    if(g_threadPool == NULL) {
        TCC_Error err = TCC_threadPoolInit();
        if(err != TCC_ERROR_MISSING) return result;
    }
    
    const size_t indicesPerThread = notesPtr->length / g_threadPool->numThreads;
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        g_threadPool->threadData[i].startIndex = i * indicesPerThread;
        g_threadPool->threadData[i].endIndex = (i == g_threadPool->numThreads - 1) ? 
            notesPtr->length - 1 : (i + 1) * indicesPerThread - 1;
        g_threadPool->threadData[i].moneyPtr = notesPtr->moneyArr;
        g_threadPool->threadData[i].datePtr = notesPtr->dateArr;
        g_threadPool->threadData[i].userData = &result;
    }
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        thrd_create(&g_threadPool->threads[i], TCC_findMaxTransaction_step, 
            &g_threadPool->threadData[i]);
    }
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        thrd_join(g_threadPool->threads[i], NULL);
    }
    
    return result;
}

TCC_YearStats* TCC_calculateYearStats(const TCC_MoneyNotes* notesPtr, size_t* numYears) {
    if(notesPtr->length == 0) {
        *numYears = 0;
        return NULL;
    }
    
    // Находим диапазон лет
    TCC_DateYear minYear = notesPtr->dateArr[0].year;
    TCC_DateYear maxYear = notesPtr->dateArr[notesPtr->length - 1].year;
    *numYears = maxYear - minYear + 1;
    
    TCC_YearStats* stats = (TCC_YearStats*)calloc(*numYears, sizeof(TCC_YearStats));
    if(!stats) return NULL;
    
    // Инициализируем начальные значения
    for(size_t i = 0; i < *numYears; ++i) {
        stats[i].min = TCC_MONEY_MAX;
        stats[i].max = 0;
    }
    
    if(g_threadPool == NULL) {
        TCC_Error err = TCC_threadPoolInit();
        if(err != TCC_ERROR_MISSING) {
            free(stats);
            return NULL;
        }
    }
    
    const size_t indicesPerThread = notesPtr->length / g_threadPool->numThreads;
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        g_threadPool->threadData[i].startIndex = i * indicesPerThread;
        g_threadPool->threadData[i].endIndex = (i == g_threadPool->numThreads - 1) ? 
            notesPtr->length - 1 : (i + 1) * indicesPerThread - 1;
        g_threadPool->threadData[i].moneyPtr = notesPtr->moneyArr;
        g_threadPool->threadData[i].datePtr = notesPtr->dateArr;
        g_threadPool->threadData[i].userData = stats;
    }
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        thrd_create(&g_threadPool->threads[i], TCC_calculateYearStats_step, 
            &g_threadPool->threadData[i]);
    }
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        thrd_join(g_threadPool->threads[i], NULL);
    }
    
    return stats;
}

TCC_MoneyNotes* TCC_filterTransactions(const TCC_MoneyNotes* notesPtr, 
                                     TCC_Money minAmount, 
                                     TCC_Money maxAmount) {
    if(notesPtr->length == 0) return NULL;
    
    // Создаем новый контейнер для отфильтрованных данных
    TCC_MoneyNotes* result = (TCC_MoneyNotes*)malloc(sizeof(TCC_MoneyNotes));
    if(!result) return NULL;
    
    result->capacity = notesPtr->length;
    result->length = 0;
    result->moneyArr = (TCC_Money*)malloc(result->capacity * sizeof(TCC_Money));
    result->dateArr = (TCC_Date*)malloc(result->capacity * sizeof(TCC_Date));
    
    if(!result->moneyArr || !result->dateArr) {
        TCC_moneyNotesDel(&result);
        return NULL;
    }
    
    TCC_FilterData filterData = {
        .resultArray = result->moneyArr,
        .resultCount = 0,
        .minAmount = minAmount,
        .maxAmount = maxAmount
    };
    
    if(g_threadPool == NULL) {
        TCC_Error err = TCC_threadPoolInit();
        if(err != TCC_ERROR_MISSING) {
            TCC_moneyNotesDel(&result);
            return NULL;
        }
    }
    
    const size_t indicesPerThread = notesPtr->length / g_threadPool->numThreads;
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        g_threadPool->threadData[i].startIndex = i * indicesPerThread;
        g_threadPool->threadData[i].endIndex = (i == g_threadPool->numThreads - 1) ? 
            notesPtr->length - 1 : (i + 1) * indicesPerThread - 1;
        g_threadPool->threadData[i].moneyPtr = notesPtr->moneyArr;
        g_threadPool->threadData[i].datePtr = notesPtr->dateArr;
        g_threadPool->threadData[i].userData = &filterData;
    }
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        thrd_create(&g_threadPool->threads[i], TCC_filterTransactions_step, 
            &g_threadPool->threadData[i]);
    }
    
    for(size_t i = 0; i < g_threadPool->numThreads; ++i) {
        thrd_join(g_threadPool->threads[i], NULL);
    }
    
    result->length = filterData.resultCount;
    return result;
}

// Дополнения 2

// Оптимизированный поиск первой записи за год
static size_t TCC_findFirstYearRecord(const TCC_MoneyNotes* moneyNotesP, int year) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || moneyNotesP->length == 0) {
        return 0;
    }

    size_t left = 0;
    size_t right = moneyNotesP->length - 1;

    while (left <= right) {
        size_t mid = left + (right - left) / 2;
        int midYear = moneyNotesP->dateArr[mid].year;

        if (midYear == year) {
            if (mid == 0 || moneyNotesP->dateArr[mid - 1].year != year) {
                return mid;
            }
            right = mid - 1;
        } else if (midYear < year) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return moneyNotesP->length;
}

// Оптимизированный поиск последней записи за год
static size_t TCC_findLastYearRecord(const TCC_MoneyNotes* moneyNotesP, int year) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || moneyNotesP->length == 0) {
        return 0;
    }

    size_t left = 0;
    size_t right = moneyNotesP->length - 1;

    while (left <= right) {
        size_t mid = left + (right - left) / 2;
        int midYear = moneyNotesP->dateArr[mid].year;

        if (midYear == year) {
            if (mid == moneyNotesP->length - 1 || moneyNotesP->dateArr[mid + 1].year != year) {
                return mid;
            }
            left = mid + 1;
        } else if (midYear < year) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return 0;
}

// Фильтрация записей по году
TCC_Error TCC_moneyNotesFilterByYear(const TCC_MoneyNotes* sourceP, TCC_MoneyNotes* destP, int year) {
    if (!sourceP || !destP || !sourceP->moneyArr) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    size_t firstIndex = TCC_findFirstYearRecord(sourceP, year);
    size_t lastIndex = TCC_findLastYearRecord(sourceP, year);

    if (firstIndex >= sourceP->length || lastIndex < firstIndex) {
        return TCC_ERROR_NOT_FOUND;
    }

    size_t count = lastIndex - firstIndex + 1;
    TCC_Error error = TCC_moneyNotesCreate(destP, count);
    if (error != TCC_ERROR_NONE) {
        return error;
    }

    memcpy(destP->moneyArr, &sourceP->moneyArr[firstIndex], count * sizeof(TCC_Money));
    destP->length = count;
    return TCC_ERROR_NONE;
}

// Изменение размера массива
TCC_Error TCC_moneyNotesResize(TCC_MoneyNotes* moneyNotesP, size_t newSize) {
    if (!moneyNotesP) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    if (newSize < moneyNotesP->length) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    TCC_Money* newMoneyArr = realloc(moneyNotesP->moneyArr, newSize * sizeof(TCC_Money));
    if (!newMoneyArr) {
        return TCC_ERROR_MEMORY_ALLOCATION;
    }

    moneyNotesP->moneyArr = newMoneyArr;
    moneyNotesP->length = newSize;
    return TCC_ERROR_NONE;
}

// Многопоточное добавление записей
TCC_Error TCC_moneyNotesInsertBatch(TCC_MoneyNotes* moneyNotesP, const TCC_Money* moneyArr, size_t count) {
    if (!moneyNotesP || !moneyArr || count == 0) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    // Проверяем, нужно ли увеличить размер массива
    if (moneyNotesP->length + count > moneyNotesP->capacity) {
        size_t newSize = moneyNotesP->capacity * 2;
        while (newSize < moneyNotesP->length + count) {
            newSize *= 2;
        }

        TCC_Error error = TCC_moneyNotesResize(moneyNotesP, newSize);
        if (error != TCC_ERROR_NONE) {
            return error;
        }
    }

    // Копируем данные
    memcpy(&moneyNotesP->moneyArr[moneyNotesP->length], moneyArr, count * sizeof(TCC_Money));
    moneyNotesP->length += count;

    return TCC_ERROR_NONE;
}

// Сортировка записей по дате
TCC_Error TCC_moneyNotesSortByDate(TCC_MoneyNotes* moneyNotesP) {
    if (!moneyNotesP || !moneyNotesP->moneyArr) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    // Сортировка пузырьком (можно заменить на более эффективный алгоритм)
    for (size_t i = 0; i < moneyNotesP->length - 1; i++) {
        for (size_t j = 0; j < moneyNotesP->length - i - 1; j++) {
            if (TCC_dateCompare(&moneyNotesP->dateArr[j], &moneyNotesP->dateArr[j + 1]) > 0) {
                TCC_Money temp = moneyNotesP->moneyArr[j];
                moneyNotesP->moneyArr[j] = moneyNotesP->moneyArr[j + 1];
                moneyNotesP->moneyArr[j + 1] = temp;
            }
        }
    }

    return TCC_ERROR_NONE;
}

// Поиск максимальной суммы за год
TCC_Error TCC_moneyNotesMaxSum(const TCC_MoneyNotes* moneyNotesP, int year, TCC_Money* maxSumP) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || !maxSumP) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    bool found = false;
    maxSumP->sum = 0;

    for (size_t i = 0; i < moneyNotesP->length; i++) {
        if (moneyNotesP->dateArr[i].year == year) {
            if (!found || moneyNotesP->moneyArr[i].sum > maxSumP->sum) {
                maxSumP->sum = moneyNotesP->moneyArr[i].sum;
                found = true;
            }
        }
    }

    return found ? TCC_ERROR_NONE : TCC_ERROR_NOT_FOUND;
}

// Вычисление средней суммы за год
TCC_Error TCC_moneyNotesAverageSum(const TCC_MoneyNotes* moneyNotesP, int year, TCC_Money* avgSumP) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || !avgSumP) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    TCC_Money sum = {0};
    size_t count = 0;

    for (size_t i = 0; i < moneyNotesP->length; i++) {
        if (moneyNotesP->dateArr[i].year == year) {
            sum.sum += moneyNotesP->moneyArr[i].sum;
            count++;
        }
    }

    if (count == 0) {
        return TCC_ERROR_NOT_FOUND;
    }

    avgSumP->sum = sum.sum / count;
    return TCC_ERROR_NONE;
}
