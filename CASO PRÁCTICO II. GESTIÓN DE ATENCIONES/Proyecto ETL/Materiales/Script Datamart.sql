drop table FactAtenciones;
drop table dimAgencia;
drop table dimItem;
drop table dimProveedor;
drop table dimEstado;
drop table dimTipoAtencion;

create table dimAgencia(
	AgenciaID INT primary KEY,
	Agencia VARCHAR(75),
	Direccion VARCHAR(255),
	Distrito VARCHAR(50),
	Provincia VARCHAR(50),
	Departamento VARCHAR(30)
);

create table dimItem(
	ItemID INT primary KEY,
	Item VARCHAR(75),
	TipoItem VARCHAR(255),
	Categoria VARCHAR(50)
);

create table dimProveedor(
	ProveedorID SERIAL primary KEY,
	Proveedor VARCHAR(75),
	TipoProveedor VARCHAR(20)
);

create table dimEstado(
	EstadoID SERIAL primary KEY,
	Estado VARCHAR(20)
);

create table dimTipoAtencion(
	TipoAtencionID SERIAL primary KEY,
	TipoAtencion VARCHAR(20)
);


create table factAtenciones(
	TicketID VARCHAR(50),
	EstadoID INT,
	TipoAtencionID INT,
	FechaAtencion DATE,
	FechaCierre DATE,
	AgenciaID INT,
	ItemID INT,
	ProveedorID INT,
	constraint fk_estadoID foreign KEY(EstadoID) references dimEstado(EstadoID),
	constraint fk_tipoAtencionID foreign KEY(TipoAtencionID) references dimTipoAtencion(TipoAtencionID),
	constraint fk_proveedorID foreign KEY(ProveedorID) references dimProveedor(ProveedorID),
	constraint fk_agenciaID foreign KEY(AgenciaID) references dimAgencia(AgenciaID),
	constraint fk_itemID foreign KEY(ItemID) references dimItem(ItemID)
)

