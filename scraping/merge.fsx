#load "../analysis/packages/FsLab/FsLab.fsx"
open System
open System.Collections.Generic
open Deedle

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__

let scrapes = 
  [ "2020-05-17"; "2020-06-01"; "2020-06-14"; "2020-06-29"; "2020-07-12"; "2020-07-26"; 
    "2020-08-09"; "2020-08-23"; "2020-09-06"; "2020-09-20"; "2020-10-04"; "2020-10-18"; 
    "2020-11-01"; "2020-11-15"; "2020-11-29"; "2020-12-13"; "2020-12-27"; "2021-01-10"; 
    "2021-01-24"; "2021-02-07"; "2021-02-21"; "2021-03-07"; "2021-03-21"; "2021-04-04"; 
    "2021-04-18"; "2021-05-02"; "2021-05-16"; "2021-05-30"; "2021-06-13"; "2021-06-27"; 
    "2021-07-11"; "2021-07-25"; "2021-08-08"; "2021-08-22"; "2021-09-05"; "2021-09-20"; 
    "2021-10-03"; "2021-10-17"; "2021-10-31"; "2021-11-14"; "2021-11-28" ]
    
let mergeFiles (files:seq<string * string>) = 
  let res = Dictionary<_, _>()
  for s, f in files do
    let df = Frame.ReadCsv(f,inferTypes=false)
    if not (df.Columns.ContainsKey("Title")) then
      df.AddColumn("Title", df.GetColumn<string>("Link") |> Series.map (fun _ _ -> ""))
    if not (df.Columns.ContainsKey("Description")) then
      df.AddColumn("Description", df.GetColumn<string>("Link") |> Series.map (fun _ _ -> ""))

    for r in df.Rows.Values do
      let url = r.GetAs<string>("Link")
      //if String.IsNullOrEmpty (r.GetAs<string>("Created")) then failwithf "EMpty: %s" f
      res.[url] <- 
        Series.merge (series ["Source" => box s])
          r.[["Link"; "Title"; "Description"; "Created"; "MostRecentDonation"; "Donations"; "Raised"; "Complete"]] 
  Frame.ofRows [ for (KeyValue(k,v)) in res -> k => v ]
  |> Frame.dropSparseRows

let files = 
  [ for s in ["gofundme"; "just"; "virgin"] do
    for d in scrapes do 
    for k in ["foodbank"; "food-bank"; "soup-kitchen"; "homeless"] do
    yield s, sprintf "../outputs/%s/%s_%s.csv" d s k ]

let filesExist = 
  [ for s, f in files do
      if not (IO.File.Exists(f)) then 
        printfn "File does not exist: %s" f
      else yield s, f ]

let merged = mergeFiles filesExist

let rowsAt d = 
  let files = 
    [ for s in ["gofundme"; "just"; "virgin"] do
      for k in ["foodbank"; "food-bank"; "soup-kitchen"; "homeless"] do
      let fn = sprintf "../outputs/%s/%s_%s.csv" d s k
      if IO.File.Exists fn then
        yield s, fn ]
  (mergeFiles files).Rows.Values

let removed datePairs = 
  [| for od, nd in datePairs do
      let o, n = rowsAt od, rowsAt nd
      let nd = DateTime.Parse nd
      let nks = set [ for r in n -> r.GetAs<string>("Link") ]
      for r in o do
        if r.GetAs "Created" <> "" && not (nks.Contains(r.GetAs<string>("Link"))) then
          let dt = r.GetAs<DateTime> "Created"
          yield r.GetAs<string> "Link", (nd, int (nd - dt).TotalDays / 7) |]

let removedSeries = 
  removed (Seq.pairwise scrapes)  
  |> Array.map (fun (k, (dt, _)) -> k, dt.ToString("yyyy-MM-dd")) 
  |> Array.distinctBy fst
  |> series

let withRemoved = merged |> Frame.addCol "Removed" removedSeries
withRemoved.SaveCsv("../outputs/merged.csv")
