(*** hide ***)
#load "packages/FsLab/Themes/DefaultWhite.fsx"
#load "packages/FsLab/FsLab.fsx"
open System
open Deedle
open XPlot.GoogleCharts

Environment.CurrentDirectory <- __SOURCE_DIRECTORY__
(**

Testing
=======
*)
let gf1 = Frame.ReadCsv("../outputs/2020-06-14/gofundme_food-bank.csv")

let parseDonations (s:string) =
  s.Split([|'/'|], StringSplitOptions.RemoveEmptyEntries) |> Array.map (fun s -> 
    let kv = s.Split(' ')
    DateTime.Parse kv.[0], int kv.[1])

gf1 
|> Stats.median

let traces = 
  gf1.Rows 
  |> Series.filter (fun _ row -> 
      let dons = row.GetAs<string>"Details.Donations" |> parseDonations
      let raised = row.GetAs<int>"Details.Raised"
      //raised > 325 &&
      dons.Length <> 0 && dons |> Seq.sumBy snd = raised )
  |> Series.map (fun _ row ->
      let dons = row.GetAs<string>"Details.Donations" |> parseDonations
      let created = row.GetAs<DateTime>"Details.Created"
      let dons = dons |> Array.map (fun (d, v) -> max 0 (int (d - created).TotalDays), v)
      let max = dons |> Seq.map fst |> Seq.max
      let w = 7
      let res = Array.zeroCreate ((max + 1) / w + 1)
      for i, d in dons do res.[i/w] <- res.[i/w] + d
      res )
  |> Series.values

traces
|> Seq.reduce (fun a1 a2 -> Array.init (max a1.Length a2.Length) (fun i ->
    (if i < a1.Length then a1.[i] else 0) + (if i < a2.Length then a2.[i] else 0) ))
//|> Array.indexed
//|> Array.filter (fun (i, v) -> v <> 0)
//|> Array.map (fun (i, v) -> i, log (float v))
|> Chart.Line

let ch = 
  Chart.Line [ for t in Seq.truncate 100 traces -> Seq.truncate 20 (Seq.indexed t) ]
(*** include-value:ch ***)



//let gof1 = Frame.ReadCsv("../outputs/2020-06-14/gofundme_food-bank.csv")
//let gof2 = Frame.ReadCsv("../outputs/2020-06-14/gofundme_foodbank.csv")

let gof1 = Frame.ReadCsv("../outputs/2020-06-01/gofundme_foodbank.csv")
let gof2 = Frame.ReadCsv("../outputs/2020-06-14/gofundme_foodbank.csv")
let gof1k = gof1 |> Frame.indexRowsString "Url"
let gof2k = gof2 |> Frame.indexRowsString "Url"

let overlap = Set.intersect (set gof1k.RowKeys) (set gof2k.RowKeys) |> Array.ofSeq

gof1.RowCount
gof2.RowCount
overlap.Length

overlap |> Seq.countBy (fun k -> gof1k.Rows.[k] = gof2k.Rows.[k])

let differ = overlap |> Seq.filter (fun k -> gof1k.Rows.[k] <> gof2k.Rows.[k])
gof1k.Rows.[Seq.item 10 differ]
gof2k.Rows.[Seq.item 10 differ]

for k in overlap do 
  printfn "%s\n%A" k (gof1k.Rows.[k] = gof2k.Rows.[k])
