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

// Группировка записей по месяцам
TCC_Error TCC_moneyNotesGroupByMonth(const TCC_MoneyNotes* moneyNotesP, TCC_MoneyNotes* resultP, int year) {
    if (!moneyNotesP || !resultP || !moneyNotesP->moneyArr) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    // Создаем массив для хранения сумм по месяцам
    TCC_Money monthlySums[12] = {0};
    bool hasData[12] = {false};

    // Собираем суммы по месяцам
    for (size_t i = 0; i < moneyNotesP->length; i++) {
        if (moneyNotesP->dateArr[i].year == year) {
            int month = moneyNotesP->dateArr[i].month - 1;
            monthlySums[month].sum += moneyNotesP->moneyArr[i].sum;
            hasData[month] = true;
        }
    }

    // Подсчитываем количество месяцев с данными
    size_t count = 0;
    for (int i = 0; i < 12; i++) {
        if (hasData[i]) count++;
    }

    // Создаем результирующий массив
    TCC_Error error = TCC_moneyNotesCreate(resultP, count);
    if (error != TCC_ERROR_NONE) {
        return error;
    }

    // Заполняем результирующий массив
    size_t index = 0;
    for (int i = 0; i < 12; i++) {
        if (hasData[i]) {
            resultP->dateArr[index].year = year;
            resultP->dateArr[index].month = i + 1;
            resultP->dateArr[index].day = 1;
            resultP->moneyArr[index].sum = monthlySums[i].sum;
            index++;
        }
    }

    resultP->length = count;
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

// Уплотнение массива
TCC_Error TCC_moneyNotesCompact(TCC_MoneyNotes* moneyNotesP) {
    if (!moneyNotesP || !moneyNotesP->moneyArr) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    if (moneyNotesP->length == moneyNotesP->capacity) {
        return TCC_ERROR_NONE;
    }

    return TCC_moneyNotesResize(moneyNotesP, moneyNotesP->length);
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

// Проверка сортировки записей
bool TCC_moneyNotesIsSorted(const TCC_MoneyNotes* moneyNotesP) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || moneyNotesP->length <= 1) {
        return true;
    }

    for (size_t i = 0; i < moneyNotesP->length - 1; i++) {
        if (TCC_dateCompare(&moneyNotesP->dateArr[i], &moneyNotesP->dateArr[i + 1]) > 0) {
            return false;
        }
    }

    return true;
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

// Поиск минимальной суммы за год
TCC_Error TCC_moneyNotesMinSum(const TCC_MoneyNotes* moneyNotesP, int year, TCC_Money* minSumP) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || !minSumP) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    bool found = false;
    minSumP->sum = 0;

    for (size_t i = 0; i < moneyNotesP->length; i++) {
        if (moneyNotesP->dateArr[i].year == year) {
            if (!found || moneyNotesP->moneyArr[i].sum < minSumP->sum) {
                minSumP->sum = moneyNotesP->moneyArr[i].sum;
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

// SIMD версия функции суммирования по диапазону
TCC_Error TCC_moneyNotesRangeSumSIMD(const TCC_MoneyNotes* moneyNotesP, const TCC_Date* startDateP, const TCC_Date* endDateP, TCC_Money* sumP) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || !startDateP || !endDateP || !sumP) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    sumP->sum = 0;

    // Используем SSE для векторизации суммирования
    #ifdef __SSE2__
    __m128d sumVec = _mm_setzero_pd();
    size_t i;
    
    for (i = 0; i + 2 <= moneyNotesP->length; i += 2) {
        if (TCC_dateCompare(&moneyNotesP->dateArr[i], startDateP) >= 0 &&
            TCC_dateCompare(&moneyNotesP->dateArr[i], endDateP) <= 0) {
            __m128d values = _mm_setr_pd(moneyNotesP->moneyArr[i].sum, moneyNotesP->moneyArr[i + 1].sum);
            sumVec = _mm_add_pd(sumVec, values);
        }
    }
    
    double temp[2];
    _mm_storeu_pd(temp, sumVec);
    sumP->sum = temp[0] + temp[1];
    
    // Обрабатываем оставшиеся элементы
    for (; i < moneyNotesP->length; i++) {
        if (TCC_dateCompare(&moneyNotesP->dateArr[i], startDateP) >= 0 &&
            TCC_dateCompare(&moneyNotesP->dateArr[i], endDateP) <= 0) {
            sumP->sum += moneyNotesP->moneyArr[i].sum;
        }
    }
    #else
    // Стандартная версия без SIMD
    for (size_t i = 0; i < moneyNotesP->length; i++) {
        if (TCC_dateCompare(&moneyNotesP->dateArr[i], startDateP) >= 0 &&
            TCC_dateCompare(&moneyNotesP->dateArr[i], endDateP) <= 0) {
            sumP->sum += moneyNotesP->moneyArr[i].sum;
        }
    }
    #endif

    return TCC_ERROR_NONE;
}

// Улучшенная многопоточная версия суммирования по диапазону
TCC_Error TCC_moneyNotesRangeSumParallel(const TCC_MoneyNotes* moneyNotesP, const TCC_Date* startDateP, const TCC_Date* endDateP, TCC_Money* sumP) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || !startDateP || !endDateP || !sumP) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    // Определяем оптимальное количество потоков
    size_t numThreads = TCC_getOptimalThreadCount();
    if (moneyNotesP->length < numThreads * 1000) {
        // Для малых наборов данных используем последовательный алгоритм
        return TCC_moneyRangeSum(moneyNotesP, 0, moneyNotesP->length - 1, sumP);
    }

    // Создаем массив для хранения частичных сумм
    TCC_Money* partialSums = calloc(numThreads, sizeof(TCC_Money));
    if (!partialSums) {
        return TCC_ERROR_MEMORY_ALLOCATION;
    }

    // Создаем и запускаем потоки
    thrd_t* threads = malloc(numThreads * sizeof(thrd_t));
    if (!threads) {
        free(partialSums);
        return TCC_ERROR_MEMORY_ALLOCATION;
    }

    size_t chunkSize = moneyNotesP->length / numThreads;
    for (size_t i = 0; i < numThreads; i++) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? moneyNotesP->length : (i + 1) * chunkSize;
        
        TCC_RangeSumThreadData* data = malloc(sizeof(TCC_RangeSumThreadData));
        if (!data) {
            // Очищаем ресурсы и возвращаем ошибку
            for (size_t j = 0; j < i; j++) {
                thrd_join(threads[j], NULL);
            }
            free(threads);
            free(partialSums);
            return TCC_ERROR_MEMORY_ALLOCATION;
        }

        data->moneyNotesP = moneyNotesP;
        data->startDateP = startDateP;
        data->endDateP = endDateP;
        data->startIndex = start;
        data->endIndex = end;
        data->sumP = &partialSums[i];

        if (thrd_create(&threads[i], TCC_rangeSumThread, data) != thrd_success) {
            free(data);
            // Очищаем ресурсы и возвращаем ошибку
            for (size_t j = 0; j < i; j++) {
                thrd_join(threads[j], NULL);
            }
            free(threads);
            free(partialSums);
            return TCC_ERROR_THREAD_CREATION;
        }
    }

    // Ожидаем завершения всех потоков
    for (size_t i = 0; i < numThreads; i++) {
        thrd_join(threads[i], NULL);
    }

    // Суммируем частичные результаты
    sumP->sum = 0;
    for (size_t i = 0; i < numThreads; i++) {
        sumP->sum += partialSums[i].sum;
    }

    // Освобождаем ресурсы
    free(threads);
    free(partialSums);

    return TCC_ERROR_NONE;
}

// Сохранение в бинарный файл
TCC_Error TCC_moneyNotesSaveBinary(const TCC_MoneyNotes* moneyNotesP, const char* filename) {
    if (!moneyNotesP || !moneyNotesP->moneyArr || !filename) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    FILE* file = fopen(filename, "wb");
    if (!file) {
        return TCC_ERROR_FILE_OPERATION;
    }

    // Записываем количество записей
    if (fwrite(&moneyNotesP->length, sizeof(size_t), 1, file) != 1) {
        fclose(file);
        return TCC_ERROR_FILE_OPERATION;
    }

    // Записываем данные
    if (fwrite(moneyNotesP->moneyArr, sizeof(TCC_Money), moneyNotesP->length, file) != moneyNotesP->length) {
        fclose(file);
        return TCC_ERROR_FILE_OPERATION;
    }

    fclose(file);
    return TCC_ERROR_NONE;
}

// Загрузка из бинарного файла
TCC_Error TCC_moneyNotesLoadBinary(TCC_MoneyNotes* moneyNotesP, const char* filename) {
    if (!moneyNotesP || !filename) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    FILE* file = fopen(filename, "rb");
    if (!file) {
        return TCC_ERROR_FILE_OPERATION;
    }

    // Читаем количество записей
    size_t count;
    if (fread(&count, sizeof(size_t), 1, file) != 1) {
        fclose(file);
        return TCC_ERROR_FILE_OPERATION;
    }

    // Создаем массив нужного размера
    TCC_Error error = TCC_moneyNotesCreate(moneyNotesP, count);
    if (error != TCC_ERROR_NONE) {
        fclose(file);
        return error;
    }

    // Читаем данные
    if (fread(moneyNotesP->moneyArr, sizeof(TCC_Money), count, file) != count) {
        TCC_moneyNotesDel(moneyNotesP);
        fclose(file);
        return TCC_ERROR_FILE_OPERATION;
    }

    moneyNotesP->length = count;
    fclose(file);
    return TCC_ERROR_NONE;
}

// Атомарное добавление нескольких записей
TCC_Error TCC_moneyNotesInsertTransaction(TCC_MoneyNotes* moneyNotesP, const TCC_Money* moneyArr, size_t count) {
    if (!moneyNotesP || !moneyArr || count == 0) {
        return TCC_ERROR_INVALID_PARAMETER;
    }

    // Создаем временный массив для хранения текущих данных
    TCC_Money* tempMoneyArr = malloc(moneyNotesP->length * sizeof(TCC_Money));
    if (!tempMoneyArr) {
        return TCC_ERROR_MEMORY_ALLOCATION;
    }

    // Копируем текущие данные
    memcpy(tempMoneyArr, moneyNotesP->moneyArr, moneyNotesP->length * sizeof(TCC_Money));
    size_t oldCount = moneyNotesP->length;

    // Увеличиваем размер массива
    TCC_Error error = TCC_moneyNotesResize(moneyNotesP, moneyNotesP->length + count);
    if (error != TCC_ERROR_NONE) {
        free(tempMoneyArr);
        return error;
    }

    // Добавляем новые записи
    memcpy(&moneyNotesP->moneyArr[moneyNotesP->length], moneyArr, count * sizeof(TCC_Money));
    moneyNotesP->length += count;

    // Проверяем корректность данных
    if (!TCC_moneyNotesValidate(moneyNotesP)) {
        // В случае ошибки восстанавливаем предыдущее состояние
        memcpy(moneyNotesP->moneyArr, tempMoneyArr, oldCount * sizeof(TCC_Money));
        moneyNotesP->length = oldCount;
        free(tempMoneyArr);
        return TCC_ERROR_INVALID_DATA;
    }

    free(tempMoneyArr);
    return TCC_ERROR_NONE;
}
