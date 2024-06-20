package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/deephaven/deephaven-core/go/pkg/client"
)

func main() {
	doStuff3()
}

func doStuff3() {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, "localhost", "10000")
	fmt.Println(err)
	defer cl.Close()

	tbl, err := cl.OpenTable(ctx, "t")
	if err != nil {
		fmt.Println(err)
		return
	}

	tickingTbl, updateChan, err := tbl.Subscribe(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	for update := range updateChan {
		if update.Err != nil {
			fmt.Println("oh no! ", update.Err)
			break
		}
		update := update.Update

		// do some calculations on tickingTbl

		//for r := range update.RemovedRows.GetAllRows() {
		//mySum -= tickingTbl.GetRowByKey(r)
		//}

		fmt.Println(update.Record)

		tickingTbl.ApplyUpdate(update)

		//for r := range update.AddedRows.GetAllRows() {
		//mySum += tickingTbl.GetRowByKey(r)
		//}

		// do some calculations on the changed table

		fmt.Println(tickingTbl)
	}

	err = tbl.Release(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func doStuff2() {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, "localhost", "10000")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer cl.Close()

	tbls, err := cl.ExecBatch(ctx, cl.TimeTableQuery(time.Second, time.Now()).Tail(15))
	if err != nil {
		fmt.Println(err)
		return
	}
	tbl := tbls[0]
	defer tbl.Release(ctx)

	tickingTbl, updateCh, err := tbl.Subscribe(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	for update := range updateCh {
		if update.Err != nil {
			fmt.Println("oh no! ", update.Err)
			break
		}
		update := update.Update

		tickingTbl.ApplyUpdate(update)
		fmt.Println(tickingTbl)
	}

}

func doStuff1() {
	ctx := context.Background()

	cl, err := client.NewClient(ctx, "localhost", "10000")
	fmt.Println(err)
	defer cl.Close()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "K", Type: arrow.PrimitiveTypes.Int32},
			{Name: "V1", Type: arrow.PrimitiveTypes.Int32},
			{Name: "V2", Type: arrow.PrimitiveTypes.Int32},
		},
		nil,
	)

	tbl, err := cl.NewKeyBackedInputTableFromSchema(ctx, schema, "K")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer tbl.Release(ctx)

	tbl2, err := cl.NewKeyBackedInputTableFromSchema(ctx, schema, "K")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer tbl2.Release(ctx)

	// If you run this query for long enough, it tests ragged modifies (modifies with columns of differing lengths).
	//tbl3node := tbl.Query().Join(tbl2.Query().Update("FOO = V1 + V2"), []string{"K"}, []string{"W1 = V1", "W2 = V2", "FOO"}, 10)

	// Uncomment this line to just test modifies instead.
	/*tbl3node := tbl.Query()

	tbls, err := cl.ExecBatch(ctx, tbl3node)
	if err != nil {
		fmt.Println(err)
		return
	}
	tbl3 := tbls[0]
	defer tbl3.Release(ctx)*/

	/*tbl, err := cl.OpenTable(ctx, "t")
	if err != nil {
		fmt.Println(err)
		return
	}*/

	/*tickingTbl, updateChan, err := cl.Subscribe(ctx, tbl3)
	if err != nil {
		fmt.Println(err)
		return
	}*/

	//mySum := 0

	startTime := time.Now()

	go func() {
		b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
		defer b.Release()

		i := int32(1)

		for {
			b.Field(0).(*array.Int32Builder).AppendValues([]int32{int32(rand.Intn(5))}, nil)
			b.Field(1).(*array.Int32Builder).AppendValues([]int32{int32(rand.Intn(2))}, nil)
			b.Field(2).(*array.Int32Builder).AppendValues([]int32{i}, nil)

			rec1 := b.NewRecord()

			b.Field(0).(*array.Int32Builder).AppendValues([]int32{int32(rand.Intn(5))}, nil)
			b.Field(1).(*array.Int32Builder).AppendValues([]int32{int32(rand.Intn(2))}, nil)
			b.Field(2).(*array.Int32Builder).AppendValues([]int32{i}, nil)

			rec2 := b.NewRecord()

			fmt.Println("uploaded ", i)

			i++

			dur := time.Since(startTime)
			fmt.Println("average time: ", float64(dur.Milliseconds())/float64(i-1))

			recTbl1, err := cl.ImportTable(ctx, rec1)
			if err != nil {
				fmt.Println("IMPORT", err)
				return
			}
			recTbl2, err := cl.ImportTable(ctx, rec2)
			if err != nil {
				fmt.Println("IMPORT", err)
				return
			}

			err = tbl.AddTable(ctx, recTbl1)
			if err != nil {
				fmt.Println("ADDTABLE", err)
				return
			}
			err = tbl2.AddTable(ctx, recTbl2)
			if err != nil {
				fmt.Println("ADDTABLE", err)
				return
			}

			rec1.Release()
			rec2.Release()
			err = recTbl1.Release(ctx)
			if err != nil {
				fmt.Println("RELEASE", err)
				return
			}
			err = recTbl2.Release(ctx)
			if err != nil {
				fmt.Println("RELEASE", err)
				return
			}

			time.Sleep(time.Millisecond * 10)
		}
	}()

	for {
		time.Sleep(time.Second)
	}

	/*for update := range updateChan {
		if update.Err != nil {
			fmt.Println("oh no! ", update.Err)
			break
		}
		update := update.Update

		// do some calculations on tickingTbl

		//for r := range update.RemovedRows.GetAllRows() {
		//mySum -= tickingTbl.GetRowByKey(r)
		//}

		tickingTbl.ApplyUpdate(update)

		//for r := range update.AddedRows.GetAllRows() {
		//mySum += tickingTbl.GetRowByKey(r)
		//}

		// do some calculations on the changed table

		fmt.Println(tickingTbl)
	}*/
}
