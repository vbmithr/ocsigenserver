(* Arakoon boilerplate code from http://arakoon.org/documentation/arakoon_ocaml_client.html *)

open Lwt
open Arakoon_remote_client
open Arakoon_client

let make_address ip port =
  let ha = Unix.inet_addr_of_string ip in
  Unix.ADDR_INET (ha,port)

let with_client cluster_id (ip,port) f =
  let sa = make_address ip port in
  let do_it connection =
    make_remote_client cluster_id connection >>= fun client ->
    f client
  in
  Lwt_io.with_connection sa do_it

let find_master cluster_id cfgs =
  let rec loop = function
    | [] -> Lwt.fail (Failure "too many nodes down")
    | cfg :: rest ->
      begin
        let _,(ip, port) = cfg in
        let sa = make_address ip port in
        Lwt.catch
          (fun () ->
             Lwt_io.with_connection sa
               (fun connection ->
                  make_remote_client cluster_id connection
                  >>= fun client ->
                  client # who_master ()) >>= function
             | None -> Lwt.fail (Failure "No Master")
             | Some m -> Lwt.return m)
          (function
            | Unix.Unix_error(Unix.ECONNREFUSED,_,_ ) -> loop rest
            | exn -> Lwt.fail exn
          )
      end
  in loop cfgs


let with_master_client cluster_id cfgs f =
  find_master cluster_id cfgs >>= fun master_name ->
  let master_cfg = List.assoc master_name cfgs in
  with_client cluster_id master_cfg f

type protocol =
  | Get_nodename
  | Get_cluster_id
  | Get_nodes
  | Nodename of string
  | Cluster_id of string
  | Nodes of (string * (string * int)) list with json

let get_cluster_id (ic,oc) =
  Lwt_io.write oc (Json_protocol.to_string Get_cluster_id) >>= fun () ->
  Lwt_io.read ic >>= fun s -> match (Json_protocol.from_string s) with
  | Cluster_id cid -> Lwt.return cid
  | _ -> Lwt.fail (Invalid_argument "protocol mismatch")

let get_nodes (ic,oc) =
  Lwt_io.write oc (Json_protocol.to_string Get_nodes) >>= fun () ->
  Lwt_io.read ic >>= fun s -> match (Json_protocol.from_string s) with
  | Nodes ns -> Lwt.return ns
  | _ -> Lwt.fail (Invalid_argument "protocol mismatch")

let manager_ip = ref Unix.inet_addr_loopback
let manager_port = ref 4444

let with_managed f =
  Lwt_io.with_connection
    Unix.(ADDR_INET(!manager_ip, !manager_port))
    (fun chans ->
       get_cluster_id chans >>= fun cid ->
       get_nodes chans >>= fun cfgs ->
       with_master_client cid cfgs f
    )

let store_name store name = store ^ "___" ^ name

(* Ocsipersist protocol *)

type 'a t = string * string

type store = string

let open_store a : store = a

let get (store, name) =
  with_managed (fun c ->
      c#get (store_name store name) >>= fun v -> Lwt.return (Marshal.from_string v 0))


let set (store, name) value =
  with_managed (fun c -> store_name store name |> fun k ->
                 c#set k (Marshal.to_string value []))


let make_persistent_lazy_lwt ~store ~name ~default =
  with_managed (fun c -> store_name store name |> fun k ->
                 Lwt.catch
                   (fun () -> c#aSSert_exists k >>= fun () -> Lwt.return (store, name))
                   (fun _ -> default () >>= fun v ->
                     c#set k (Marshal.to_string v []) >>= fun () ->
                     Lwt.return (store, name))
               )

let make_persistent_lazy ~store ~name ~default =
  let default () = Lwt.wrap default in
  make_persistent_lazy_lwt ~store ~name ~default

let make_persistent ~store ~name ~default =
  make_persistent_lazy ~store ~name ~default:(fun () -> default)

type 'value table = string

let table_name t = Lwt.return t

let open_table t = t

let find t k = Lwt.catch
    (fun () -> get (t, k))
    (function
      | Arakoon_exc.Exception (Arakoon_exc.E_NOT_FOUND, _) -> Lwt.fail Not_found
      | exn -> Lwt.fail exn)

let add t k v = set (t, k) v

let replace_if_exists store name value =
  with_managed (fun c -> store_name store name |> fun k ->
                 Lwt.catch
                   (fun () -> c#aSSert_exists k >>= fun () ->
                     c#set k (Marshal.to_string value []))
                   (fun _ -> Lwt.fail Not_found))

let remove store name =
  with_managed (fun c -> store_name store name |> c#delete)

let length t =
  with_managed (fun c -> c#prefix_keys t (-1) >>= fun ks -> List.length ks |> Lwt.return)

let iter_step f t =
  with_managed (fun c -> c#prefix_keys t (-1) >>= fun ks ->
                 c#multi_get ks >>= fun vs ->
                 let vs = List.map (fun v -> Marshal.from_string v 0) vs in
                 let pairs = List.combine ks vs in
                 Lwt_list.iter_s (fun (k,s) -> f k s) pairs)

let iter_table = iter_step

let fold_step f t a =
  with_managed (fun c -> c#prefix_keys t (-1) >>= fun ks ->
                 c#multi_get ks >>= fun vs ->
                 let vs = List.map (fun v -> Marshal.from_string v 0) vs in
                 let pairs = List.combine ks vs in
                 Lwt_list.fold_left_s (fun a (k,v) -> f k v a) a pairs)

let fold_table = fold_step

let iter_block f t =
  with_managed (fun c -> c#prefix_keys t (-1) >>= fun ks ->
                 c#multi_get ks >>= fun vs ->
                 let vs = List.map (fun v -> Marshal.from_string v 0) vs in
                 let pairs = List.combine ks vs in
                 List.iter (fun (k,v) -> f k v) pairs;
                 Lwt.return ())

(* Initialisation *)

open Simplexmlparser
let rec init_fun = function
  | [] -> ()
  | Element ("ip", _, [PCData ip])::t ->
    manager_ip := Unix.inet_addr_of_string ip;
    init_fun t
  | Element ("port", _, [PCData port])::t ->
    manager_port := int_of_string port;
    init_fun t
  | _ -> raise (Ocsigen_extensions.Error_in_config_file
                  ("Unexpected content inside Ocsipersist config"))

let _ = Ocsigen_extensions.register_extension ~name:"ocsipersist" ~init_fun ()
